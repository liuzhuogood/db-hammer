import json
from pathlib import Path

import pytest

import db_hammer.mcp.tools  # noqa: F401
from db_hammer.mcp import DBHammerMCPServer


@pytest.fixture()
def server(tmp_path: Path):
    config = {
        "connections": {},
        "default_connection": None,
        "auth": {
            "api_keys": ["web-key"],
            "jwt_secret": "web-secret",
            "token_expire_seconds": 3600,
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    srv = DBHammerMCPServer(config_path=str(config_path))
    client = srv.test_client()
    return srv, client


def test_tool_execution(server, tmp_path: Path):
    srv, client = server
    headers = {"X-API-Key": "web-key", "Content-Type": "application/json"}

    tools_response = client.get("/api/tools", headers=headers)
    assert tools_response.status_code == 200

    db_path = tmp_path / "http.db"
    payload = json.dumps({"database_path": str(db_path)})
    response = client.post("/api/tools/create_sqlite_connection", headers=headers, data=payload)
    data = response.get_json()
    assert response.status_code == 200
    connection_id = data["data"]["connection_id"]

    create_table = json.dumps({"connection_id": connection_id, "sql": "CREATE TABLE demo(id INTEGER)"})
    client.post("/api/tools/execute_query", headers=headers, data=create_table)

    insert_payload = json.dumps(
        {"connection_id": connection_id, "table": "demo", "data": {"id": 1}},
    )
    client.post("/api/tools/insert_data", headers=headers, data=insert_payload)

    query_payload = json.dumps({"connection_id": connection_id, "table": "demo"})
    select_response = client.post("/api/tools/execute_select", headers=headers, data=query_payload)
    assert select_response.get_json()["data"]["count"] == 1

    resources = client.get(f"/api/resources/tables?connection_id={connection_id}", headers=headers)
    assert resources.status_code == 200


def test_config_api(server):
    srv, client = server
    headers = {"X-API-Key": "web-key", "Content-Type": "application/json"}
    config_response = client.get("/api/config", headers=headers)
    assert config_response.status_code == 200

    update_response = client.post("/api/config", headers=headers, data=json.dumps({"default_connection": None}))
    assert update_response.status_code == 200


def test_token_issue(server):
    srv, client = server
    token_response = client.post("/api/auth/token", data=json.dumps({"api_key": "web-key"}), headers={"Content-Type": "application/json"})
    assert token_response.status_code == 200
    assert "token" in token_response.get_json()
