"""Query execution tools for the MCP server."""
from __future__ import annotations

from typing import Dict, List, Optional

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


@tool
def execute_query(connection_id: str, sql: str, params: Optional[Dict[str, object]] = None) -> List[dict]:
    """Execute an arbitrary SQL query and return results as dictionaries."""

    _validator.ensure_safe_sql(sql)
    with use_connection(connection_id) as connection:
        rows = connection.select_dict_list(sql, params=params)
        return rows


@tool
def execute_select(
    connection_id: str,
    table: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[dict]:
    """Execute a SELECT query for a table."""

    selected_columns = columns or ["*"]
    if columns:
        _validator.ensure_columns(columns)
    sql = f"SELECT {', '.join(selected_columns)} FROM {table}"
    if where:
        _validator.ensure_safe_sql(where)
        sql += f" WHERE {where}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return execute_query(connection_id, sql)


@tool
def execute_query_with_pagination(
    connection_id: str,
    sql: str,
    page_size: int = 100,
    page: int = 1,
) -> Dict[str, object]:
    """Execute a paginated query returning metadata."""

    _validator.ensure_safe_sql(sql)
    if page < 1:
        raise MCPError("页码不能小于1")
    if page_size <= 0:
        raise MCPError("分页大小必须大于0")
    offset = (page - 1) * page_size
    paginated_sql = f"SELECT * FROM ( {sql} ) paginated LIMIT {page_size} OFFSET {offset}"
    with use_connection(connection_id) as connection:
        rows = connection.select_dict_list(paginated_sql)
        total_pages, total_rows = connection.select_page_size(sql, page_size)
        return {
            "rows": rows,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_rows": total_rows,
        }


@tool
def explain_query(connection_id: str, sql: str) -> Dict[str, object]:
    """Explain the query plan for a SQL statement."""

    _validator.ensure_safe_sql(sql)
    with use_connection(connection_id) as connection:
        db_type = connection.db_type
        if db_type.lower() == "sqlite":
            explain_sql = f"EXPLAIN QUERY PLAN {sql}"
        else:
            explain_sql = f"EXPLAIN {sql}"
        rows = connection.select_dict_list(explain_sql)
        return {"db_type": db_type, "plan": rows}


__all__ = [
    "execute_query",
    "execute_select",
    "execute_query_with_pagination",
    "explain_query",
]
