"""Flask application factory for the MCP configuration UI."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from flask import Flask, render_template

from .config_manager import WebConfigManager

from .api import api_blueprint


def create_app(config_path: Optional[str] = None) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).with_name("templates")),
        static_folder=str(Path(__file__).with_name("static")),
    )
    app.config["mcp_config_manager"] = WebConfigManager(config_path)
    app.register_blueprint(api_blueprint)

    @app.route("/")
    def index():
        return render_template("config.html")

    @app.route("/health")
    def health():
        return {"status": "ok"}

    return app


__all__ = ["create_app"]
