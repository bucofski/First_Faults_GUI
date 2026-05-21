import logging
import os
import tomllib
from pathlib import Path

from flask import Flask, render_template, url_for, redirect

from config.logging_config import setup_logging
from presentations.routes import plc_routes
from presentations.routes import auth_routes
from presentations.services import auth_service

_app_log = logging.getLogger("presentations")


def create_app() -> Flask:
    setup_logging()

    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-only-change-in-prod")
    app.jinja_options["autoescape"] = True
    app.register_blueprint(auth_routes.bp)
    app.register_blueprint(plc_routes.bp)

    auth_service.init_oauth(app)

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
