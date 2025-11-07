"""Web配置API"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from ..auth import AuthManager
from ..config import ConfigManager
from ..exceptions import AuthenticationError, MCPServerError
from .config_manager import WebConfigManager
from .connection_tester import test_connection_params
from .simple_app import Blueprint, SimpleRequest, SimpleResponse, jsonify

if TYPE_CHECKING:
    from ..tools.connection import ConnectionRegistry


def create_api_blueprint(
    config_manager: ConfigManager,
    registry: "ConnectionRegistry",
    auth_manager: AuthManager,
) -> Blueprint:
    web_manager = WebConfigManager(config_manager)
    blueprint = Blueprint("mcp_api", url_prefix="/api")

    def _require_auth(headers: Dict[str, str]) -> str:
        return auth_manager.authenticate_headers(headers)

    @blueprint.route("/config", methods=["GET"])
    def get_config(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
        _require_auth(request.headers)
        return jsonify(web_manager.get_config())

    @blueprint.route("/config", methods=["POST"])
    def update_config(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
        _require_auth(request.headers)
        config = web_manager.update_config(request.json())
        return jsonify(config)

    @blueprint.route("/config/test", methods=["POST"])
    def test_connection(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
        _require_auth(request.headers)
        result = test_connection_params(registry, request.json())
        return jsonify(result)

    @blueprint.route("/connections/active", methods=["GET"])
    def active_connections(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
        _require_auth(request.headers)
        return jsonify(registry.list_connections())

    @blueprint.route("/auth/token", methods=["POST"])
    def issue_token(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
        data = request.json()
        api_key = data.get("api_key")
        if not api_key:
            raise MCPServerError("缺少api_key")
        try:
            auth_manager.authenticate_headers({"X-API-Key": api_key})
        except AuthenticationError as exc:  # noqa: PERF203
            raise MCPServerError(str(exc)) from exc
        token = auth_manager.issue_token(api_key)
        return jsonify({"token": token})

    return blueprint


__all__ = ["create_api_blueprint"]
