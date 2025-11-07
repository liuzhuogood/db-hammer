from pathlib import Path

from db_hammer.mcp.server import create_mcp_app
from db_hammer.mcp.fastmcp_adapter import FastMCP


def test_create_mcp_app(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text('{"host": "0.0.0.0", "port": 8123}', encoding="utf-8")
    app, config = create_mcp_app(str(config_path))
    assert isinstance(app, FastMCP)
    assert config.port == 8123
    assert "create_sqlite_connection" in app.registry.tools or not app.registry.tools
    assert "tables" in app.registry.resources
