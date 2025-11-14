import tomllib
from pathlib import Path

from flask import Flask, url_for, redirect, session

from presentations import plc
from presentations.services.creadential import Role
from presentations.services.credential_service import CredentialService


def create_app() -> Flask:
    app = Flask("app")
    app.secret_key = "dev"
    app.jinja_options["autoescape"] = True
    app.register_blueprint(plc.bp)

    config_path = (Path(__file__).resolve().parent.parent / "config" / "config.toml")

    loaded = {}
    if config_path.is_file():
        with open(config_path, "rb") as f:
            loaded = tomllib.load(f)

    server_cfg = loaded.get("server", {})
    # Provide safe defaults if not present
    app.config["SERVER_HOST"] = server_cfg.get("host", "127.0.0.1")
    app.config["SERVER_PORT"] = server_cfg.get("port", 5000)

    @app.before_request
    def ensure_session_credentials():
        cred = CredentialService.get_current_credential()
        if cred is not None:
            session["username"] = cred.username
            role_obj = cred.role
            session["role"] = role_obj.value if role_obj is not None else Role.GUEST.value

    @app.route("/ping")
    def ping():
        return "pong"

    @app.route("/")
    def start():
        try:
            return redirect(url_for("plc.home"))
        except Exception:
            return redirect(url_for("ping"))

    return app
