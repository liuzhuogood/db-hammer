from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from db_hammer.mcp.tools import crud, query, schema
from db_hammer.mcp.tools.connection import ConnectionManager, configure_manager
from db_hammer.mcp.tools.export import ExportManager, configure_export_manager, export_table_data


@pytest.fixture
def sqlite_connection(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, quantity INTEGER)")
    conn.commit()
    conn.close()

    manager = ConnectionManager()
    configure_manager(manager)
    connection_id = manager.create_connection("sqlite", database=str(db_path))

    export_manager = ExportManager(str(tmp_path / "exports"))
    configure_export_manager(export_manager)

    yield connection_id

    manager.close(connection_id)


def test_execute_select(sqlite_connection):
    rows = query.execute_select(sqlite_connection, "items")
    assert rows == []


def test_crud_cycle(sqlite_connection):
    inserted = crud.insert_data(
        sqlite_connection,
        "items",
        {"name": "Widget", "quantity": 2},
    )
    assert inserted == 1

    updated = crud.update_data(
        sqlite_connection,
        "items",
        {"quantity": 5},
        "name = 'Widget'",
    )
    assert updated == 1

    rows = query.execute_select(sqlite_connection, "items")
    assert rows[0]["quantity"] == 5

    deleted = crud.delete_data(sqlite_connection, "items", "name = 'Widget'")
    assert deleted == 1


def test_export_table(sqlite_connection, tmp_path):
    crud.insert_data(
        sqlite_connection,
        "items",
        {"name": "Widget", "quantity": 2},
    )
    result = export_table_data(sqlite_connection, "items")
    assert result["status"] == "completed"
    file_path = Path(result["file_path"])
    assert file_path.exists()


def test_schema_introspection(sqlite_connection):
    tables = schema.list_tables(sqlite_connection)
    assert any(row.get("table_name") == "items" or row.get("name") == "items" for row in tables)
    details = schema.describe_table(sqlite_connection, "items")
    column_names = {row.get("name") or row.get("column_name") for row in details}
    assert "name" in column_names
