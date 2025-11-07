"""Flask API routes for MCP configuration."""
from __future__ import annotations

from flask import Blueprint, jsonify, request

from .config_manager import ConfigManager
from .connection_tester import ConnectionTester

api_bp = Blueprint("mcp_api", __name__, url_prefix="/api")
manager = ConfigManager()
tester = ConnectionTester()


@api_bp.route("/config", methods=["GET"])
def get_config():
    config = manager.load()
    return jsonify(config.to_dict())


@api_bp.route("/config", methods=["POST"])
def save_config():
    data = request.get_json(force=True)
    config = manager.save(data)
    return jsonify(config.to_dict())


@api_bp.route("/test-connection", methods=["POST"])
def test_connection():
    payload = request.get_json(force=True)
    db_type = payload.get("type")
    params = payload.get("params", {})
    result = tester.test(db_type, **params)
    return jsonify({"success": result.success, "message": result.message})
