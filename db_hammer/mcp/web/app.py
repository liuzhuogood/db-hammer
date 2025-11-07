"""Flask application factory for the MCP configuration UI."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

try:  # pragma: no cover - optional dependency
    from flask import Flask, render_template
except Exception:  # pragma: no cover - optional
    Flask = None  # type: ignore
    render_template = None  # type: ignore

from ..config import ConfigManager
from ..tools.connection import ConnectionManager, configure_manager
from ..tools.export import ExportManager, configure_export_manager
from .api import create_api_blueprint
from .config_manager import WebConfigManager


def create_app(
    config_path: Optional[str] = None,
    connection_manager: Optional[ConnectionManager] = None,
    export_manager: Optional[ExportManager] = None,
) -> Flask:
    if Flask is None:
        raise RuntimeError("Flask is required to use the web console")
    base_path = Path(__file__).resolve().parents[2]
    template_folder = base_path / "web" / "templates"
    static_folder = base_path / "web" / "static"

    app = Flask(__name__, template_folder=str(template_folder), static_folder=str(static_folder))

    config_manager = WebConfigManager(ConfigManager(config_path))
    connection_manager = connection_manager or ConnectionManager(config_manager.manager.load())
    export_manager = export_manager or ExportManager()
    configure_manager(connection_manager)
    configure_export_manager(export_manager)

    app.register_blueprint(
        create_api_blueprint(config_manager, export_manager, connection_manager)
    )

    @app.route("/")
    def index():  # pragma: no cover - template rendering
        return render_template("config.html")

    return app
