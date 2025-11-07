import json
import sqlite3
from pathlib import Path

import pytest

from db_hammer.mcp.auth import AuthManager
from db_hammer.mcp.config import ConfigManager
from db_hammer.mcp.export import ExportManager
from db_hammer.mcp.server import CURRENT_CONTEXT, ToolContext
from db_hammer.mcp.tools import connection as connection_tools
from db_hammer.mcp.tools import crud as crud_tools
from db_hammer.mcp.tools import export as export_tools
from db_hammer.mcp.tools import query as query_tools
from db_hammer.mcp.tools import schema as schema_tools


@pytest.fixture()
def tool_context(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        json.dumps(
            {
                "connections": {
                    "default_sqlite": {
                        "name": "default_sqlite",
                        "type": "SQLITE",
                        "params": {"database": str(tmp_path / "test.db")},
                    }
                },
                "default_connection": "default_sqlite",
                "auth": {"api_keys": ["test"], "jwt_secret": "secret", "token_expire_seconds": 3600},
            }
        ),
        encoding="utf-8",
    )
    manager = ConfigManager(config_path=str(config_path))
    registry = connection_tools.ConnectionRegistry(manager)
    export_manager = ExportManager(str(tmp_path / "exports"))
    auth_manager = AuthManager(manager)
    context = ToolContext(config_manager=manager, export_manager=export_manager, auth_manager=auth_manager, registry=registry)
    token = CURRENT_CONTEXT.set(context)
    try:
        yield context
    finally:
        CURRENT_CONTEXT.reset(token)


@pytest.fixture()
def sqlite_connection(tool_context: ToolContext):
    db_path = Path(tool_context.config_manager.get_connection("default_sqlite").params["database"])
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()
    response = connection_tools.create_sqlite_connection(database_path=str(db_path))
    return response["connection_id"]


def test_crud_and_query(tool_context: ToolContext, sqlite_connection: str):
    connection_id = sqlite_connection

    crud_tools.insert_data(connection_id, "items", {"id": 1, "name": "hammer"})
    crud_tools.insert_data(connection_id, "items", {"id": 2, "name": "db"})

    result = query_tools.execute_select(connection_id, "items", columns=["id", "name"], limit=10)
    assert result["count"] == 2

    crud_tools.update_data(connection_id, "items", {"name": "hammer-db"}, "id = 1")
    updated = query_tools.execute_select(connection_id, "items", where="id = 1")
    assert updated["data"][0]["name"] == "hammer-db"

    crud_tools.upsert_data(connection_id, "items", {"id": 2, "name": "db-hammer"}, ["id"])
    upserted = query_tools.execute_select(connection_id, "items", where="id = 2")
    assert upserted["data"][0]["name"] == "db-hammer"

    pagination = query_tools.execute_query_with_pagination(connection_id, "SELECT * FROM items", page_size=1, page=1)
    assert pagination["total_rows"] == 2

    explain = query_tools.explain_query(connection_id, "SELECT * FROM items")
    assert "plan" in explain

    crud_tools.delete_data(connection_id, "items", "id = 2")
    remaining = query_tools.execute_select(connection_id, "items")
    assert remaining["count"] == 1


def test_export_and_resources(tool_context: ToolContext, sqlite_connection: str, tmp_path: Path):
    connection_id = sqlite_connection
    crud_tools.insert_data(connection_id, "items", {"id": 10, "name": "export"})

    export_result = export_tools.export_table_data(connection_id, "items")
    assert export_result["status"] == "completed"
    assert Path(export_result["file_path"]).exists()

    files = export_tools.list_export_files()
    assert files["items"]

    tables = schema_tools.list_tables(connection_id)
    assert "items" in tables["tables"]

    columns = schema_tools.describe_table(connection_id, "items")
    assert columns["columns"]

    schemas = schema_tools.list_schemas(connection_id)
    assert "main" in schemas["schemas"]

    export_tools.delete_export_file(export_result["export_id"])


def test_connection_registry(tool_context: ToolContext, sqlite_connection: str):
    connections = connection_tools.list_connections()
    assert sqlite_connection in connections
    connection_tools.close_connection(sqlite_connection)
    connections = connection_tools.list_connections()
    assert sqlite_connection not in connections
