"""REST API for the MCP web configuration UI."""
from __future__ import annotations

from flask import Blueprint, jsonify, request, current_app

from ..exceptions import ConfigurationError
from .config_manager import WebConfigManager

api_blueprint = Blueprint("mcp_api", __name__, url_prefix="/api")


def _manager() -> WebConfigManager:
    return current_app.config.setdefault("mcp_config_manager", WebConfigManager())


@api_blueprint.route("/config", methods=["GET"])
def get_config():
    try:
        config = _manager().load()
    except ConfigurationError as exc:
        config = _manager().generate_default()
    return jsonify(config.to_dict())


@api_blueprint.route("/config", methods=["POST"])
def save_config():
    data = request.get_json(force=True)
    try:
        config_dict = data if isinstance(data, dict) else {}
        config = _manager().save(config_dict)
        return jsonify({"success": True, "config": config.to_dict()})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 400


@api_blueprint.route("/test-connections", methods=["POST"])
def test_connections():
    data = request.get_json(force=True)
    connections = data.get("connections", []) if isinstance(data, dict) else []
    results = _manager().test_connections(connections)
    return jsonify({"results": results})


__all__ = ["api_blueprint"]
