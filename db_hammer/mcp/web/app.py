"""Web界面初始化"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict

from ..auth import AuthManager
from ..config import ConfigManager
from ..export import ExportManager
from .api import create_api_blueprint
from .simple_app import Blueprint, SimpleRequest, SimpleResponse

WEB_ROOT = Path(__file__).resolve().parents[3] / "web"

if TYPE_CHECKING:
    from ..tools.connection import ConnectionRegistry


def _serve_file(file_path: Path, content_type: str) -> SimpleResponse:
    if not file_path.exists():
        return SimpleResponse(status=404, body="Not Found", headers={})
    data = file_path.read_bytes()
    return SimpleResponse(status=200, body=data, headers={"Content-Type": content_type})


def create_web_blueprint(
    config_manager: ConfigManager,
    registry: "ConnectionRegistry",
    export_manager: ExportManager,
    auth_manager: AuthManager,
) -> Blueprint:
    blueprint = Blueprint("mcp_web", url_prefix="")
    api_blueprint = create_api_blueprint(config_manager, registry, auth_manager)
    blueprint.register_blueprint(api_blueprint)

    @blueprint.route("/", methods=["GET"])
    def index(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
        html = (WEB_ROOT / "templates" / "config.html").read_text(encoding="utf-8")
        return SimpleResponse(status=200, body=html, headers={"Content-Type": "text/html; charset=utf-8"})

    @blueprint.route("/static/<folder>/<filename>", methods=["GET"])
    def static_files(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:
        folder = params["folder"]
        filename = params["filename"]
        file_path = WEB_ROOT / "static" / folder / filename
        content_type = "text/plain"
        if filename.endswith(".css"):
            content_type = "text/css; charset=utf-8"
        elif filename.endswith(".js"):
            content_type = "application/javascript; charset=utf-8"
        return _serve_file(file_path, content_type)

    return blueprint


__all__ = ["create_web_blueprint"]
