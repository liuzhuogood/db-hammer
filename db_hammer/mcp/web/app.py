"""Flask application providing a minimal MCP configuration UI."""
from __future__ import annotations

from pathlib import Path

from flask import Flask, render_template

from .api import api_bp
from .config_manager import ConfigManager, ensure_default_config

BASE_DIR = Path(__file__).parent


def create_web_app(config_path: str | None = None) -> Flask:
    ensure_default_config(config_path)
    template_folder = BASE_DIR / "templates"
    static_folder = BASE_DIR / "static"
    app = Flask(__name__, template_folder=str(template_folder), static_folder=str(static_folder))
    app.register_blueprint(api_bp)

    @app.route("/")
    def index():
        manager = ConfigManager(config_path)
        config = manager.load()
        return render_template("config.html", config=config)

    return app
