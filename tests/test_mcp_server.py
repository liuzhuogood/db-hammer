from __future__ import annotations

from db_hammer.mcp.server import DBHammerMCPServer


def test_server_lists_tools_and_resources(tmp_path):
    config_path = tmp_path / "config.json"
    import json

    config_path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "id": "sqlite",
                        "type": "sqlite",
                        "database": str(tmp_path / "db.sqlite"),
                    }
                ],
                "authentication": [],
            }
        ),
        encoding="utf-8",
    )
    server = DBHammerMCPServer(config_path=str(config_path))
    tools = server.list_tools()
    resources = server.list_resources()
    assert "create_sqlite_connection" in tools
    assert "tables" in resources
