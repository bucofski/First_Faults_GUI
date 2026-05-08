import logging
import os
import secrets
import tomllib
from pathlib import Path

from flask import Flask, render_template, url_for, redirect, session

from config.logging_config import setup_logging
from presentations.routes import auth_routes, plc_routes
from presentations.services.auth import current_user, login_required

_app_log = logging.getLogger("presentations")


def create_app() -> Flask:
    setup_logging()

    app = Flask("app")
    secret_key = os.environ.get("FLASK_SECRET_KEY")
    if not secret_key:
        if os.environ.get("FLASK_ENV") == "production":
            raise RuntimeError(
                "FLASK_SECRET_KEY must be set in production. "
                "Generate one with: python -c 'import secrets; print(secrets.token_hex(32))'"
            )
        secret_key = secrets.token_hex(32)
        _app_log.warning("FLASK_SECRET_KEY not set; generated an ephemeral key for this process only")
    app.secret_key = secret_key
    app.jinja_options["autoescape"] = True

    # Gate every plc.* route behind authentication. Auth blueprint stays public.
    plc_routes.bp.before_request(login_required(lambda: None))

    app.register_blueprint(auth_routes.bp)
    app.register_blueprint(plc_routes.bp)

    config_path = (Path(__file__).resolve().parent.parent / "config" / "config.toml")

    try:
        with open(config_path, "rb") as f:
            loaded = tomllib.load(f)
    except FileNotFoundError:
        _app_log.error("Config file not found: %s — using defaults", config_path)
        loaded = {}
    except tomllib.TOMLDecodeError as e:
        _app_log.error("Invalid TOML in %s: %s — using defaults", config_path, e)
        loaded = {}

    server_cfg = loaded.get("server", {})
    app.config["SERVER_HOST"] = server_cfg.get("host", "127.0.0.1")
    app.config["SERVER_PORT"] = server_cfg.get("port", 5000)

    def _csrf_token() -> str:
        if "_csrf_token" not in session:
            session["_csrf_token"] = os.urandom(32).hex()
        return session["_csrf_token"]

    app.jinja_env.globals["csrf_token"] = _csrf_token

    @app.context_processor
    def inject_user():
        return {"current_user": current_user()}

    @app.errorhandler(404)
    def not_found(e):
        return render_template(
            "error.html", title="Error",
            error_code=404,
            error_title="Page Not Found",
            error_message="The page you are looking for does not exist.",
        ), 404

    @app.errorhandler(500)
    def internal_error(e):
        app.logger.error("Internal server error: %s", e, exc_info=True)
        return render_template(
            "error.html", title="Error",
            error_code=500,
            error_title="Internal Server Error",
            error_message="Something went wrong. The error has been logged.",
        ), 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        app.logger.error("Unhandled exception: %s", e, exc_info=True)
        return render_template(
            "error.html", title="Error",
            error_code=500,
            error_title="Unexpected Error",
            error_message="An unexpected error occurred. The error has been logged.",
        ), 500

    @app.route("/ping")
    def ping():
        return "pong"

    @app.route("/")
    def start():
       return redirect(url_for("plc.home"))

    return app
