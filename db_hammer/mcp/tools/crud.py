"""CRUD helper tools."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping

from ..exceptions import QueryError
from ..utils import ensure_sql_is_safe
from . import registry
from .connection import get_manager


def _placeholder(driver: str) -> str:
    return "?" if driver == "sqlite" else "%s"


def _execute_write(connection_id: str, sql: str, params: Iterable[Any]) -> int:
    ensure_sql_is_safe(sql)
    manager = get_manager()
    record = manager.get(connection_id)
    cursor = record.handle.cursor()
    try:
        cursor.execute(sql, tuple(params))
        record.handle.commit()
    except Exception as exc:  # pragma: no cover - driver specific
        record.handle.rollback()
        raise QueryError(str(exc)) from exc
    return cursor.rowcount


@registry.tool(name="insert_data", description="Insert a row into a table")
def insert_data(connection_id: str, table: str, data: Mapping[str, Any]) -> int:
    if not data:
        raise QueryError("Data payload may not be empty")
    manager = get_manager()
    record = manager.get(connection_id)
    columns = list(data.keys())
    placeholders = ", ".join([_placeholder(record.driver)] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    return _execute_write(connection_id, sql, data.values())


@registry.tool(name="update_data", description="Update rows in a table")
def update_data(connection_id: str, table: str, data: Mapping[str, Any], where: str) -> int:
    if not data:
        raise QueryError("Data payload may not be empty")
    if not where:
        raise QueryError("WHERE clause is required for updates")
    manager = get_manager()
    record = manager.get(connection_id)
    placeholders = [_placeholder(record.driver) for _ in data]
    assignments = ", ".join(f"{column} = {placeholder}" for column, placeholder in zip(data.keys(), placeholders))
    sql = f"UPDATE {table} SET {assignments} WHERE {where}"
    params = list(data.values())
    return _execute_write(connection_id, sql, params)


@registry.tool(name="delete_data", description="Delete rows from a table")
def delete_data(connection_id: str, table: str, where: str) -> int:
    if not where:
        raise QueryError("WHERE clause is required for deletions")
    sql = f"DELETE FROM {table} WHERE {where}"
    return _execute_write(connection_id, sql, ())


@registry.tool(name="upsert_data", description="Upsert a row into a table")
def upsert_data(
    connection_id: str,
    table: str,
    data: Mapping[str, Any],
    conflict_columns: Iterable[str],
) -> int:
    manager = get_manager()
    record = manager.get(connection_id)
    if record.driver != "sqlite":
        raise QueryError("Upsert is currently only implemented for SQLite")
    if not conflict_columns:
        raise QueryError("At least one conflict column is required")

    columns = list(data.keys())
    placeholders = ", ".join([_placeholder(record.driver)] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    update_assignments = ", ".join(f"{col} = excluded.{col}" for col in columns)
    sql += f" ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {update_assignments}"
    return _execute_write(connection_id, sql, data.values())
