"""Flask blueprint providing HTTP endpoints for the MCP server."""

from __future__ import annotations

from flask import Blueprint, jsonify, request, send_file

from ..exceptions import ConfigurationError, ConnectionError, ExportError
from ..tools.connection import ConnectionManager
from ..tools.export import ExportManager
from .config_manager import WebConfigManager
from .connection_tester import test_connection


def create_api_blueprint(
    config_manager: WebConfigManager,
    export_manager: ExportManager,
    connection_manager: ConnectionManager,
) -> Blueprint:
    blueprint = Blueprint("mcp_api", __name__, url_prefix="/api")

    @blueprint.errorhandler(ConfigurationError)
    @blueprint.errorhandler(ConnectionError)
    @blueprint.errorhandler(ExportError)
    def handle_error(error):  # pragma: no cover - simple glue
        response = jsonify({"error": str(error)})
        response.status_code = 400
        return response

    @blueprint.route("/config", methods=["GET"])
    def get_config():
        return jsonify(config_manager.load())

    @blueprint.route("/config", methods=["POST"])
    def update_config():
        payload = request.get_json(force=True) or {}
        config = config_manager.save(payload)
        return jsonify(config.to_dict())

    @blueprint.route("/connections/test", methods=["POST"])
    def api_test_connection():
        payload = request.get_json(force=True) or {}
        db_type = payload.pop("type", None)
        if not db_type:
            return jsonify({"error": "type is required"}), 400
        result = test_connection(db_type, **payload)
        return jsonify(result)

    @blueprint.route("/connections", methods=["GET"])
    def list_connections():
        return jsonify(connection_manager.list())

    @blueprint.route("/exports", methods=["GET"])
    def list_exports():
        return jsonify(export_manager.list_exports())

    @blueprint.route("/exports", methods=["POST"])
    def create_export():
        payload = request.get_json(force=True) or {}
        connection_id = payload.get("connection_id")
        export_type = payload.get("export_type")
        params = payload.get("params", {})
        if not connection_id or not export_type:
            return jsonify({"error": "connection_id and export_type are required"}), 400
        export_id = export_manager.create_export_task(connection_id, export_type, params)
        result = export_manager.execute_export(export_id)
        return jsonify(result)

    @blueprint.route("/exports/<export_id>", methods=["GET"])
    def get_export(export_id: str):
        return jsonify(export_manager.get_export(export_id).__dict__)

    @blueprint.route("/exports/<export_id>", methods=["DELETE"])
    def delete_export(export_id: str):
        export_manager.delete_export(export_id)
        return ("", 204)

    @blueprint.route("/exports/download/<export_id>", methods=["GET"])
    def download_export(export_id: str):
        task = export_manager.get_export(export_id)
        if not task.file_path:
            return jsonify({"error": "file not ready"}), 400
        return send_file(task.file_path, as_attachment=True)

    return blueprint
