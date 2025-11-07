import sqlite3
from pathlib import Path

import pytest

from db_hammer.mcp.tools.connection import (
    close_connection,
    create_sqlite_connection,
    list_connections,
)
from db_hammer.mcp.tools import query, crud, export, schema


@pytest.fixture()
def sqlite_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    conn.commit()
    conn.close()
    return db_path


def test_connection_and_query(sqlite_db: Path):
    connection_id = create_sqlite_connection(str(sqlite_db))
    try:
        assert connection_id in list_connections()

        crud.insert_data(connection_id, "users", {"name": "Alice", "age": 30})
        crud.insert_data(connection_id, "users", {"name": "Bob", "age": 25})

        rows = query.execute_select(connection_id, "users", columns=["name", "age"], limit=10)
        assert any(row["name"] == "Alice" for row in rows)

        paged = query.execute_query_with_pagination(connection_id, "SELECT * FROM users", page_size=1, page=1)
        assert paged["total_rows"] == 2

        plan = query.explain_query(connection_id, "SELECT * FROM users")
        assert plan["db_type"].lower() == "sqlite"

        structure = schema.get_table_schema(connection_id, "users")
        assert structure["columns"]
    finally:
        close_connection(connection_id)


def test_upsert_and_export(sqlite_db: Path, tmp_path: Path):
    connection_id = create_sqlite_connection(str(sqlite_db))
    try:
        crud.insert_data(connection_id, "users", {"id": 1, "name": "Alice", "age": 30})
        crud.upsert_data(connection_id, "users", {"id": 1, "name": "Alice", "age": 31}, ["id"])
        user = query.execute_select(connection_id, "users", where="id = 1")
        assert user[0]["age"] == 31

        export._MANAGER = export.ExportManager(storage_path=str(tmp_path))
        result = export.export_table_data(connection_id, "users")
        export_file = Path(export.download_export_file(result["export_id"]))
        assert export_file.exists()
        assert export_file.read_text().strip()
    finally:
        close_connection(connection_id)
