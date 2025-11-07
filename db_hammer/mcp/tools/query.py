"""SQL execution tools."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from ..exceptions import QueryError
from ..utils import ensure_sql_is_safe, format_rows
from . import registry
from .connection import ConnectionManager, get_manager


def _execute(connection_id: str, sql: str, params: Optional[Dict[str, Any]] = None):
    ensure_sql_is_safe(sql)
    manager = get_manager()
    record = manager.get(connection_id)
    cursor = record.handle.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
    except Exception as exc:  # pragma: no cover - DB driver errors
        raise QueryError(str(exc)) from exc
    columns = [description[0] for description in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    return columns, rows


@registry.tool(name="execute_query", description="Execute an arbitrary SQL statement")
def execute_query(connection_id: str, sql: str, params: Optional[Dict[str, Any]] = None) -> list:
    columns, rows = _execute(connection_id, sql, params)
    return format_rows(columns, rows)


@registry.tool(name="execute_select", description="Execute a SELECT statement against a table")
def execute_select(
    connection_id: str,
    table: str,
    columns: Optional[list[str]] = None,
    where: Optional[str] = None,
    limit: Optional[int] = None,
) -> list:
    cols = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return execute_query(connection_id, sql)


@registry.tool(name="execute_query_with_pagination", description="Execute a paginated SQL query")
def execute_query_with_pagination(
    connection_id: str,
    sql: str,
    page_size: int = 100,
    page: int = 1,
    params: Optional[Dict[str, Any]] = None,
) -> dict:
    ensure_sql_is_safe(sql)
    offset = max(page - 1, 0) * page_size
    paginated_sql = f"{sql} LIMIT {page_size} OFFSET {offset}"
    data = execute_query(connection_id, paginated_sql, params=params)
    return {
        "page": page,
        "page_size": page_size,
        "items": data,
    }


@registry.tool(name="explain_query", description="Explain an SQL statement")
def explain_query(connection_id: str, sql: str) -> dict:
    ensure_sql_is_safe(sql)
    manager = get_manager()
    record = manager.get(connection_id)
    driver = record.driver
    explain_sql = sql
    if driver == "sqlite":
        explain_sql = f"EXPLAIN QUERY PLAN {sql}"
    elif driver in {"mysql", "postgresql"}:
        explain_sql = f"EXPLAIN {sql}"
    elif driver == "oracle":
        explain_sql = f"EXPLAIN PLAN FOR {sql}"
    elif driver == "mssql":
        explain_sql = f"SET SHOWPLAN_ALL ON; {sql}; SET SHOWPLAN_ALL OFF"

    columns, rows = _execute(connection_id, explain_sql)
    return {
        "driver": driver,
        "sql": explain_sql,
        "plan": format_rows(columns, rows),
    }
