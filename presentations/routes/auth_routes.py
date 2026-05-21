import functools
import logging

from flask import (
    Blueprint,
    redirect,
    render_template,
    request,
    session,
    url_for,
    flash,
)

from presentations.services import auth_service, user_store

_log = logging.getLogger("auth")

bp = Blueprint("auth", __name__, url_prefix="/auth")


# ---------------------------------------------------------------------------
# Login-required decorator
# ---------------------------------------------------------------------------

def login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if "username" not in session:
            session["next"] = request.url
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@bp.route("/login")
def login():
    if "username" in session:
        return redirect(url_for("plc.home"))
    return render_template("auth/login.html", title="Login")


@bp.route("/google")
def google_login():
    redirect_uri = url_for("auth.callback", _external=True)
    return auth_service.google_client().authorize_redirect(redirect_uri)


@bp.route("/callback")
def callback():
    token = auth_service.google_client().authorize_access_token()
    user_info = token.get("userinfo")
    if not user_info:
        flash("Google login failed — no user info returned.", "error")
        return redirect(url_for("auth.login"))

    google_sub = user_info["sub"]
    email = user_info.get("email", "")
    name = user_info.get("name", email)

    user = user_store.upsert_user(google_sub, email, name)
    _log.info("OAuth callback: sub=%s email=%s totp_enrolled=%s", google_sub, email, user["totp_enrolled"])

    # Store partial-auth state; full session set only after TOTP
    session["oauth_pending"] = google_sub

    if user["totp_enrolled"]:
        return redirect(url_for("auth.totp_verify"))
    return redirect(url_for("auth.totp_setup"))


@bp.route("/totp-setup", methods=["GET", "POST"])
def totp_setup():
    google_sub = session.get("oauth_pending")
    if not google_sub:
        return redirect(url_for("auth.login"))

    user = user_store.get_user(google_sub)
    if user is None:
        session.pop("oauth_pending", None)
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        secret = session.get("totp_pending_secret")
        code = request.form.get("totp_code", "").strip()
        if not secret:
            flash("Session expired — please log in again.", "error")
            return redirect(url_for("auth.login"))
        if not auth_service.verify_totp(secret, code):
            flash("Invalid code. Try again.", "error")
            qr_data = auth_service.qr_code_base64(auth_service.get_totp_uri(secret, user["email"]))
            return render_template("auth/totp_setup.html", title="Set up 2FA", qr_data=qr_data, secret=secret)

        user_store.enroll_totp(google_sub, secret)
        session.pop("totp_pending_secret", None)
        _log.info("TOTP enrolled: sub=%s", google_sub)
        _complete_login(user)
        return redirect(session.pop("next", url_for("plc.home")))

    # GET — generate a new TOTP secret for this setup attempt
    secret = auth_service.generate_totp_secret()
    session["totp_pending_secret"] = secret
    qr_data = auth_service.qr_code_base64(auth_service.get_totp_uri(secret, user["email"]))
    return render_template("auth/totp_setup.html", title="Set up 2FA", qr_data=qr_data, secret=secret)


@bp.route("/totp-verify", methods=["GET", "POST"])
def totp_verify():
    google_sub = session.get("oauth_pending")
    if not google_sub:
        return redirect(url_for("auth.login"))

    user = user_store.get_user(google_sub)
    if user is None or not user["totp_enrolled"]:
        return redirect(url_for("auth.totp_setup"))

    if request.method == "POST":
        code = request.form.get("totp_code", "").strip()
        if not auth_service.verify_totp(user["totp_secret"], code):
            flash("Invalid code. Try again.", "error")
            return render_template("auth/totp_verify.html", title="Two-Factor Authentication")

        _log.info("TOTP verified: sub=%s", google_sub)
        _complete_login(user)
        return redirect(session.pop("next", url_for("plc.home")))

    return render_template("auth/totp_verify.html", title="Two-Factor Authentication")


@bp.route("/logout")
def logout():
    username = session.get("username", "unknown")
    _log.info("Logout: user=%s", username)
    session.clear()
    return redirect(url_for("auth.login"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _complete_login(user: dict) -> None:
    session.pop("oauth_pending", None)
    session["username"] = user["email"]
    session["role"] = user["role"]
    session["google_sub"] = user["google_sub"]
