from pathlib import Path

import pytest

from db_hammer.mcp.server import create_mcp_app
from db_hammer.mcp.tools.connection import registry
from db_hammer.mcp.tools.export import ExportManager


@pytest.fixture(scope="module", autouse=True)
def _setup_sqlite(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("db") / "test.sqlite"
    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.executemany("INSERT INTO items (name) VALUES (?)", [("foo",), ("bar",)])
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture(scope="module")
def mcp_app():
    app, _ = create_mcp_app()
    return app


def test_connection_lifecycle(_setup_sqlite):
    connection_id = registry.create_connection("sqlite", database=str(_setup_sqlite))
    assert connection_id in registry.list_connections()
    assert registry.close_connection(connection_id)


def test_query_and_crud(mcp_app, _setup_sqlite):
    connection_id = registry.create_connection("sqlite", database=str(_setup_sqlite))
    insert = mcp_app.registry.tools["insert_data"]
    update = mcp_app.registry.tools["update_data"]
    delete = mcp_app.registry.tools["delete_data"]
    execute_query = mcp_app.registry.tools["execute_query"]

    insert(connection_id=connection_id, table="items", data={"id": 3, "name": "baz"})
    update(connection_id=connection_id, table="items", data={"name": "baz-updated"}, where="id = 3")
    rows = execute_query(connection_id=connection_id, sql="SELECT * FROM items WHERE id = 3")
    assert rows[0]["name"] == "baz-updated"
    delete(connection_id=connection_id, table="items", where="id = 3")
    registry.close_connection(connection_id)


def test_export_manager(tmp_path, _setup_sqlite):
    manager = ExportManager(str(tmp_path))
    connection_id = registry.create_connection("sqlite", database=str(_setup_sqlite))
    result = manager.create_export_task(connection_id, "table", {"table": "items", "format": "csv"})
    export = manager.execute_export(result)
    assert export["status"] == "completed"
    assert Path(export["file_path"]).exists()
    registry.close_connection(connection_id)
