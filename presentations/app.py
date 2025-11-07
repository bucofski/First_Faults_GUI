import tomllib

from flask import Flask, Blueprint, url_for, redirect

from presentations import plc


def create_app() -> Flask:
    app = Flask("app")
    app.secret_key = "dev"
    app.jinja_options["autoescape"] = True
    app.register_blueprint(plc.bp)
    app.config.from_file("../config/config.toml", load=tomllib.load, text=False)

    @app.route("/ping")
    def ping():
        return "pong"

    @app.route("/")
    def start():
        return redirect(url_for("audit.home"))

    return app