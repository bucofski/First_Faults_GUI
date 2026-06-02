"""
Shared auth routes: login page, TOTP (2FA) enrollment / verification, logout,
and the @login_required decorator.

Provider-specific OAuth flows live in their own blueprints:
    - auth_google.py     (Google + TOTP)
    - auth_corporate.py  (local oauth2_server only, no TOTP)
"""

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

from presentations.services import totp_service, user_store

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
# Shared routes
# ---------------------------------------------------------------------------

@bp.route("/login")
def login():
    if "username" in session:
        return redirect(url_for("plc.home"))
    return render_template("auth/login.html", title="Login")


@bp.route("/logout")
def logout():
    username = session.get("username", "unknown")
    _log.info("Logout: user=%s", username)
    session.clear()
    return redirect(url_for("auth.login"))


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
        if not totp_service.verify(secret, code):
            flash("Invalid code. Try again.", "error")
            qr_data = totp_service.qr_code_base64(
                totp_service.provisioning_uri(secret, user["email"])
            )
            return render_template(
                "auth/totp_setup.html", title="Set up 2FA",
                qr_data=qr_data, secret=secret,
            )

        user_store.enroll_totp(google_sub, secret)
        session.pop("totp_pending_secret", None)
        _log.info("TOTP enrolled: sub=%s", google_sub)
        _complete_login(user)
        return redirect(session.pop("next", url_for("plc.home")))

    secret = totp_service.generate_secret()
    session["totp_pending_secret"] = secret
    qr_data = totp_service.qr_code_base64(
        totp_service.provisioning_uri(secret, user["email"])
    )
    return render_template(
        "auth/totp_setup.html", title="Set up 2FA",
        qr_data=qr_data, secret=secret,
    )


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
        if not totp_service.verify(user["totp_secret"], code):
            flash("Invalid code. Try again.", "error")
            return render_template(
                "auth/totp_verify.html", title="Two-Factor Authentication"
            )

        _log.info("TOTP verified: sub=%s", google_sub)
        _complete_login(user)
        return redirect(session.pop("next", url_for("plc.home")))

    return render_template("auth/totp_verify.html", title="Two-Factor Authentication")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _complete_login(user: dict) -> None:
    session.pop("oauth_pending", None)
    session["username"] = user["email"]
    session["role"] = user["role"]
    session["google_sub"] = user["google_sub"]
