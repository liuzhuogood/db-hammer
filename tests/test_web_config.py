from pathlib import Path

import pytest

pytest.importorskip("flask")

from db_hammer.mcp.config import MCPConfig, save_config
from db_hammer.mcp.web.app import create_app


def write_config(path: Path) -> None:
    config = MCPConfig()
    save_config(config, path)


@pytest.fixture()
def flask_app(tmp_path: Path):
    config_path = tmp_path / "mcp.yaml"
    write_config(config_path)
    app = create_app(str(config_path))
    app.config.update(TESTING=True)
    return app


def test_get_and_save_config(flask_app):
    client = flask_app.test_client()
    response = client.get("/api/config")
    assert response.status_code == 200
    payload = response.get_json()
    payload["server"]["port"] = 9999
    save_resp = client.post("/api/config", json=payload)
    assert save_resp.status_code == 200
    assert save_resp.get_json()["success"] is True


def test_connection_testing(flask_app):
    client = flask_app.test_client()
    response = client.post("/api/test-connections", json={"connections": []})
    assert response.status_code == 200
    assert response.get_json()["results"] == []
