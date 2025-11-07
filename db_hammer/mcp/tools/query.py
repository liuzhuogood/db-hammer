"""Query execution tools for MCP server."""
from __future__ import annotations

from typing import Dict, List, Optional

from ..fastmcp_adapter import FastMCP
from ..exceptions import MCPError
from ..utils.validator import ensure, require_fields
from .connection import registry


def register_tools(app: FastMCP) -> None:
    """Register SQL query tools."""

    @app.tool()
    def execute_query(connection_id: str, sql: str, params: Optional[Dict[str, object]] = None) -> List[Dict[str, object]]:
        require_fields({"connection_id": connection_id, "sql": sql}, ["connection_id", "sql"])
        connection = registry.get_connection(connection_id)
        return connection.select_dict_list(sql, params=params)

    @app.tool()
    def execute_select(
        connection_id: str,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        require_fields({"connection_id": connection_id, "table": table}, ["connection_id", "table"])
        columns_sql = ", ".join(columns) if columns else "*"
        sql = f"SELECT {columns_sql} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        connection = registry.get_connection(connection_id)
        return connection.select_dict_list(sql)

    @app.tool()
    def execute_query_with_pagination(
        connection_id: str,
        sql: str,
        page_size: int = 100,
        page: int = 1,
    ) -> Dict[str, object]:
        require_fields({"connection_id": connection_id, "sql": sql}, ["connection_id", "sql"])
        ensure(page >= 1, "Page must be >= 1")
        connection = registry.get_connection(connection_id)
        total_pages, total_rows = connection.select_page_size(sql, page_size=page_size)
        rows = connection.select_dict_page_list(sql, page_size=page_size, page_start=page)
        return {"page": page, "page_size": page_size, "total_pages": total_pages, "total_rows": total_rows, "rows": rows}

    @app.tool()
    def explain_query(connection_id: str, sql: str) -> Dict[str, object]:
        require_fields({"connection_id": connection_id, "sql": sql}, ["connection_id", "sql"])
        connection = registry.get_connection(connection_id)
        db_type = str(getattr(connection, "db_type", "")).lower()
        if db_type == "sqlite":
            explain_sql = f"EXPLAIN QUERY PLAN {sql}"
        else:
            explain_sql = f"EXPLAIN {sql}"
        rows = connection.select_dict_list(explain_sql)
        return {"db_type": db_type, "plan": rows}
