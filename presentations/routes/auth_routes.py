from flask import Blueprint, flash, redirect, render_template, request, url_for

from presentations.services.auth import (
    authenticate,
    log_failed_login,
    login_user,
    logout_user,
)

bp = Blueprint("auth", __name__, url_prefix="/auth")


@bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = authenticate(username, password)
        if user is None:
            log_failed_login(username)
            flash("Invalid username or password.", "error")
            return render_template("login.html", title="Login"), 401
        login_user(user)
        next_url = request.args.get("next") or url_for("plc.home")
        if not next_url.startswith("/"):
            next_url = url_for("plc.home")
        return redirect(next_url)

    return render_template("login.html", title="Login")


@bp.route("/logout", methods=["POST"])
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
