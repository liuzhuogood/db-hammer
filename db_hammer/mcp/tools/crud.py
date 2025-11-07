"""CRUD helper tools for the MCP server."""
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
from ..utils.validator import SQLValidator
from .connection import use_connection

_validator = SQLValidator()


def _build_insert_sql(table: str, data: Dict[str, object]) -> (str, Dict[str, object]):
    columns = list(data.keys())
    _validator.ensure_columns(columns)
    placeholders = [f":{col}" for col in columns]
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    return sql, data


def _build_update_sql(table: str, data: Dict[str, object], where: str) -> (str, Dict[str, object]):
    columns = list(data.keys())
    _validator.ensure_columns(columns)
    assignments = [f"{col} = :{col}" for col in columns]
    sql = f"UPDATE {table} SET {', '.join(assignments)} WHERE {where}"
    params = dict(data)
    return sql, params


def _build_delete_sql(table: str, where: str) -> str:
    return f"DELETE FROM {table} WHERE {where}"


@tool
def insert_data(connection_id: str, table: str, data: Dict[str, object]) -> int:
    """Insert a new row into the table."""

    if not data:
        raise MCPError("插入数据不能为空")
    sql, params = _build_insert_sql(table, data)
    with use_connection(connection_id) as connection:
        return connection.execute(sql, params)


@tool
def update_data(connection_id: str, table: str, data: Dict[str, object], where: str) -> int:
    """Update rows in a table."""

    if not data:
        raise MCPError("更新数据不能为空")
    _validator.ensure_safe_sql(where)
    sql, params = _build_update_sql(table, data, where)
    with use_connection(connection_id) as connection:
        return connection.execute(sql, params)


@tool
def delete_data(connection_id: str, table: str, where: str) -> int:
    """Delete rows from a table."""

    _validator.ensure_safe_sql(where)
    sql = _build_delete_sql(table, where)
    with use_connection(connection_id) as connection:
        return connection.execute(sql)


@tool
def upsert_data(connection_id: str, table: str, data: Dict[str, object], conflict_columns: List[str]) -> int:
    """Insert or update a row depending on conflict columns."""

    if not conflict_columns:
        raise MCPError("冲突列不能为空")
    _validator.ensure_columns(conflict_columns)
    sql, params = _build_insert_sql(table, data)
    with use_connection(connection_id) as connection:
        db_type = connection.db_type.lower()
        if db_type == "mysql":
            updates = ", ".join([f"{col}=VALUES({col})" for col in data.keys()])
            sql += f" ON DUPLICATE KEY UPDATE {updates}"
        elif db_type == "sqlite":
            updates = ", ".join([f"{col}=excluded.{col}" for col in data.keys()])
            sql += f" ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {updates}"
        else:
            raise MCPError(f"当前数据库类型不支持 UPSERT: {db_type}")
        return connection.execute(sql, params)


__all__ = ["insert_data", "update_data", "delete_data", "upsert_data"]
