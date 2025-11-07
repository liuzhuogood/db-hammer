import json
from pathlib import Path

from db_hammer.mcp.auth import AuthManager
from db_hammer.mcp.config import ConfigManager
from db_hammer.mcp.export import ExportManager
from db_hammer.mcp.tools.connection import ConnectionRegistry
from db_hammer.mcp.web.app import create_web_blueprint
from db_hammer.mcp.web.simple_app import SimpleApp


def test_web_config_endpoints(tmp_path: Path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "connections": {},
                "default_connection": None,
                "auth": {
                    "api_keys": ["console"],
                    "jwt_secret": "console-secret",
                    "token_expire_seconds": 3600,
                },
            }
        ),
        encoding="utf-8",
    )

    manager = ConfigManager(config_path=str(config_path))
    registry = ConnectionRegistry(manager)
    export_manager = ExportManager(str(tmp_path / "exports"))
    auth_manager = AuthManager(manager)

    app = SimpleApp()
    app.register_blueprint(create_web_blueprint(manager, registry, export_manager, auth_manager))
    client = app.test_client()

    headers = {"X-API-Key": "console", "Content-Type": "application/json"}
    response = client.get("/api/config", headers=headers)
    assert response.status_code == 200

    update_resp = client.post("/api/config", headers=headers, data=json.dumps({"default_connection": None}))
    assert update_resp.status_code == 200

    token_resp = client.post("/api/auth/token", data=json.dumps({"api_key": "console"}), headers={"Content-Type": "application/json"})
    assert token_resp.status_code == 200
