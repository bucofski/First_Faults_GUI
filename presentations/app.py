import logging
import tomllib
from pathlib import Path

from flask import Flask, render_template, url_for, redirect, session

from config.logging_config import setup_logging
from presentations.routes import plc_routes
from presentations.services.creadential import Role
from presentations.services.credential_service import CredentialService

_auth_log = logging.getLogger("auth")
_app_log = logging.getLogger("presentations")


def create_app() -> Flask:
    """
    Flask application factory.

    Creates and fully configures the Flask application instance:

    - Calls :func:`setup_logging` to initialise file and console logging.
    - Loads ``config/config.toml`` for server host/port settings.  If the
      file is missing or contains invalid TOML, defaults are used and an
      error is logged — the app still starts.
    - Registers the ``plc`` blueprint (all ``/plc/*`` routes).
    - Attaches a ``before_request`` hook that seeds the Flask session with
      the current user's credentials on every request.
    - Registers error handlers for 404, 500, and unhandled exceptions that
      render a consistent ``error.html`` page.

    Returns
    -------
    Flask
        A ready-to-serve Flask application.

    Notes
    -----
    ``app.secret_key`` is currently hardcoded to ``"dev"``.  In production
    this must be replaced with a strong random value set via an environment
    variable.
    """
    setup_logging()

    app = Flask("app")
    app.secret_key = "dev"
    app.jinja_options["autoescape"] = True
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

    @app.before_request
    def ensure_session_credentials():
        """
        Populate the Flask session with the current user's credentials.

        Runs before every request.  Calls :class:`CredentialService` to
        resolve the active credential and writes ``session["username"]`` and
        ``session["role"]``.  Logs a single info message the first time a
        new session is created so that login events appear in ``auth.log``.

        If ``CredentialService`` returns ``None`` (no authenticated user),
        the session is left unchanged.
        """
        cred = CredentialService.get_current_credential()
        if cred is not None:
            is_new_session = "username" not in session
            session["username"] = cred.username
            role_obj = cred.role
            session["role"] = role_obj.value if role_obj is not None else Role.GUEST.value
            if is_new_session:
                _auth_log.info("Session started: user=%s role=%s", cred.username, session["role"])

    @app.errorhandler(404)
    def not_found(e):
        """Render the 404 error page when a route is not found."""
        return render_template(
            "error.html", title="Error",
            error_code=404,
            error_title="Page Not Found",
            error_message="The page you are looking for does not exist.",
        ), 404

    @app.errorhandler(500)
    def internal_error(e):
        """Render the 500 error page and log the exception."""
        app.logger.error("Internal server error: %s", e, exc_info=True)
        return render_template(
            "error.html", title="Error",
            error_code=500,
            error_title="Internal Server Error",
            error_message="Something went wrong. The error has been logged.",
        ), 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        """Catch-all handler for any unhandled exception — logs and renders a 500 page."""
        app.logger.error("Unhandled exception: %s", e, exc_info=True)
        return render_template(
            "error.html", title="Error",
            error_code=500,
            error_title="Unexpected Error",
            error_message="An unexpected error occurred. The error has been logged.",
        ), 500

    @app.route("/ping")
    def ping():
        """Health-check endpoint. Returns ``"pong"`` with a 200 status."""
        return "pong"

    @app.route("/")
    def start():
        """Redirect the root URL to the PLC home page."""
        return redirect(url_for("plc.home"))

    return app
