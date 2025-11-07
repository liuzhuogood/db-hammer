"""Schema inspection tools."""
from __future__ import annotations

from typing import Dict, List

try:  # pragma: no cover
    from fastmcp import tool
except ImportError:  # pragma: no cover
    def tool(func=None, **kwargs):  # type: ignore
        if func is None:
            return lambda wrapped: wrapped
        return func

from ..exceptions import MCPError
from .connection import use_connection
from .query import execute_query


def _list_sqlite_tables(connection) -> List[dict]:
    rows = connection.select_dict_list("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return rows


def _describe_sqlite_table(connection, table: str) -> List[dict]:
    sql = f"PRAGMA table_info('{table}')"
    return connection.select_dict_list(sql)


@tool
def list_tables(connection_id: str) -> List[dict]:
    """List available tables for the connection."""

    with use_connection(connection_id) as connection:
        db_type = connection.db_type.lower()
        if db_type == "sqlite":
            return _list_sqlite_tables(connection)
        raise MCPError(f"暂不支持的数据库类型: {db_type}")


@tool
def describe_table(connection_id: str, table_name: str) -> List[dict]:
    """Describe a table."""

    with use_connection(connection_id) as connection:
        db_type = connection.db_type.lower()
        if db_type == "sqlite":
            return _describe_sqlite_table(connection, table_name)
        raise MCPError(f"暂不支持的数据库类型: {db_type}")


@tool
def get_table_schema(connection_id: str, table_name: str) -> Dict[str, object]:
    """Return schema information for a table."""

    description = describe_table(connection_id, table_name)
    return {"table": table_name, "columns": description}


@tool
def get_table_data(connection_id: str, table_name: str, limit: int = 100) -> List[dict]:
    """Return data rows for a table."""

    sql = f"SELECT * FROM {table_name} LIMIT {int(limit)}"
    return execute_query(connection_id, sql)


__all__ = ["list_tables", "describe_table", "get_table_schema", "get_table_data"]
