"""CRUD operations for MCP server."""
from __future__ import annotations

from typing import Dict, Iterable

from ..fastmcp_adapter import FastMCP
from ..utils.validator import ensure, require_fields
from .connection import registry


def _build_insert_sql(table: str, data: Dict[str, object]) -> tuple[str, Dict[str, object]]:
    require_fields(data, data.keys())
    columns = ", ".join(data.keys())
    placeholders = ", ".join(f":{key}" for key in data.keys())
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    return sql, data


def _build_update_sql(table: str, data: Dict[str, object], where: str) -> tuple[str, Dict[str, object]]:
    ensure(where, "Update operation requires WHERE clause")
    require_fields(data, data.keys())
    set_clause = ", ".join(f"{key} = :{key}" for key in data.keys())
    sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
    return sql, data


def _build_delete_sql(table: str, where: str) -> str:
    ensure(where, "Delete operation requires WHERE clause")
    return f"DELETE FROM {table} WHERE {where}"


def register_tools(app: FastMCP) -> None:
    @app.tool()
    def insert_data(connection_id: str, table: str, data: Dict[str, object]) -> int:
        require_fields({"connection_id": connection_id, "table": table}, ["connection_id", "table"])
        sql, params = _build_insert_sql(table, data)
        connection = registry.get_connection(connection_id)
        rows = connection.execute(sql, params)
        connection.commit()
        return rows

    @app.tool()
    def update_data(connection_id: str, table: str, data: Dict[str, object], where: str) -> int:
        require_fields({"connection_id": connection_id, "table": table}, ["connection_id", "table"])
        sql, params = _build_update_sql(table, data, where)
        connection = registry.get_connection(connection_id)
        rows = connection.execute(sql, params)
        connection.commit()
        return rows

    @app.tool()
    def delete_data(connection_id: str, table: str, where: str) -> int:
        require_fields({"connection_id": connection_id, "table": table}, ["connection_id", "table"])
        sql = _build_delete_sql(table, where)
        connection = registry.get_connection(connection_id)
        rows = connection.execute(sql)
        connection.commit()
        return rows

    @app.tool()
    def upsert_data(connection_id: str, table: str, data: Dict[str, object], conflict_columns: Iterable[str]) -> int:
        require_fields({"connection_id": connection_id, "table": table}, ["connection_id", "table"])
        ensure(conflict_columns, "Conflict columns must be provided for upsert")
        columns_list = list(conflict_columns)
        ensure(columns_list, "Conflict columns must be provided for upsert")

        connection = registry.get_connection(connection_id)
        conflict_clause = ", ".join(columns_list)
        set_clause = ", ".join(f"{key} = excluded.{key}" for key in data.keys())
        columns = ", ".join(data.keys())
        values = ", ".join(f":{key}" for key in data.keys())
        sql = (
            f"INSERT INTO {table} ({columns}) VALUES ({values}) "
            f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause}"
        )
        rows = connection.execute(sql, data)
        connection.commit()
        return rows
