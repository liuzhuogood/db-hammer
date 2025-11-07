from db_hammer.mcp.config import MCPConfig
from db_hammer.mcp.server import create_server


def test_create_server_registers_tools():
    config = MCPConfig()
    server = create_server(config)
    assert "create_sqlite_connection" in server.tools
    assert server.resources
