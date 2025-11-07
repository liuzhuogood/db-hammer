"""Schema discovery tools."""

from __future__ import annotations

from typing import Any, Dict, List

from ..exceptions import QueryError
from . import registry
from .connection import get_manager
from .query import execute_query


@registry.tool(name="list_tables", description="List tables in the connected database")
def list_tables(connection_id: str) -> List[Dict[str, Any]]:
    manager = get_manager()
    record = manager.get(connection_id)
    driver = record.driver
    if driver == "sqlite":
        sql = "SELECT name as table_name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
    elif driver == "postgresql":
        sql = (
            "SELECT table_name, table_schema FROM information_schema.tables "
            "WHERE table_type='BASE TABLE'"
        )
    elif driver == "mysql":
        sql = "SHOW FULL TABLES"
    else:
        sql = ""
    if not sql:
        raise QueryError(f"Listing tables is not implemented for driver {driver}")
    return execute_query(connection_id, sql)


@registry.tool(name="describe_table", description="Describe table columns")
def describe_table(connection_id: str, table: str) -> List[Dict[str, Any]]:
    manager = get_manager()
    record = manager.get(connection_id)
    driver = record.driver
    if driver == "sqlite":
        sql = f"PRAGMA table_info('{table}')"
        rows = execute_query(connection_id, sql)
        for row in rows:
            row.setdefault("nullable", not row.get("notnull"))
        return rows
    elif driver == "postgresql":
        sql = (
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_name = '{table}'"
        )
        return execute_query(connection_id, sql)
    elif driver == "mysql":
        sql = f"DESCRIBE {table}"
        return execute_query(connection_id, sql)
    raise QueryError(f"Describe table is not implemented for driver {driver}")


@registry.tool(name="list_indexes", description="List indexes for a table")
def list_indexes(connection_id: str, table: str) -> List[Dict[str, Any]]:
    manager = get_manager()
    record = manager.get(connection_id)
    driver = record.driver
    if driver == "sqlite":
        sql = f"PRAGMA index_list('{table}')"
        return execute_query(connection_id, sql)
    raise QueryError(f"Listing indexes is not implemented for driver {driver}")
