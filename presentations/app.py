import tomllib
from pathlib import Path

from flask import Flask, Blueprint, url_for, redirect

from presentations import plc


def create_app() -> Flask:
    app = Flask("app")
    app.secret_key = "dev"
    app.jinja_options["autoescape"] = True
    app.register_blueprint(plc.bp)

    # Resolve config path robustly relative to this file
    config_path = (Path(__file__).resolve().parent.parent / "config" / "config.toml")

    # Load config.toml with validation and defaults
    loaded = {}
    if config_path.is_file():
        with open(config_path, "rb") as f:
            loaded = tomllib.load(f)

    server_cfg = loaded.get("server", {})
    # Provide safe defaults if not present
    app.config["SERVER_HOST"] = server_cfg.get("host", "127.0.0.1")
    app.config["SERVER_PORT"] = server_cfg.get("port", 5000)

    @app.route("/ping")
    def ping():
        return "pong"

    @app.route("/")
    def start():
        # Try to redirect to a known endpoint; fall back to /ping if not present
        try:
            # Adjust this to the real endpoint in your plc blueprint (e.g., "plc.home")
            return redirect(url_for("plc.home"))
        except Exception:
            return redirect(url_for("ping"))

    return app