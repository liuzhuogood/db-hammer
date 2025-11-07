"""查询相关工具"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..server import get_tool_context, mcp_tool
from ..utils.formatter import rows_to_dict_list
from ..utils.validator import ensure_safe_sql, validate_columns, validate_table_name


def _fetch_dict_list(connection, sql: str, params: Optional[Dict[str, Any]] = None) -> List[dict]:
    cursor = connection.cursor
    cursor.execute(sql, params or {})
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    return rows_to_dict_list(columns, rows)


@mcp_tool(description="执行任意SQL")
def execute_query(connection_id: str, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ensure_safe_sql(sql)
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    sql_upper = sql.strip().upper()
    if sql_upper.startswith("SELECT"):
        data = _fetch_dict_list(connection, sql, params)
        return {"type": "select", "data": data, "count": len(data)}
    rowcount = connection.execute(sql, params)
    if not connection.auto_commit:
        connection.conn.commit()
    return {"type": "execute", "rowcount": rowcount}


@mcp_tool(description="快捷SELECT")
def execute_select(
    connection_id: str,
    table: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    validate_table_name(table)
    if columns:
        validate_columns(columns)
        column_sql = ", ".join(columns)
    else:
        column_sql = "*"
    sql = f"SELECT {column_sql} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return execute_query(connection_id=connection_id, sql=sql)


@mcp_tool(description="分页查询")
def execute_query_with_pagination(
    connection_id: str,
    sql: str,
    page_size: int = 100,
    page: int = 1,
) -> Dict[str, Any]:
    ensure_safe_sql(sql)
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    total_pages, total_rows = connection.select_page_size(sql, page_size=page_size)
    data = connection.select_dict_page_list(sql, page_size=page_size, page_start=page)
    return {"page": page, "page_size": page_size, "total_pages": total_pages, "total_rows": total_rows, "data": data}


@mcp_tool(description="Explain分析")
def explain_query(connection_id: str, sql: str) -> Dict[str, Any]:
    ensure_safe_sql(sql)
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = getattr(connection, "db_type", "").upper()
    explain_prefix = "EXPLAIN"
    if db_type == "SQLITE":
        explain_prefix = "EXPLAIN QUERY PLAN"
    explain_sql = f"{explain_prefix} {sql}"
    data = _fetch_dict_list(connection, explain_sql)
    return {"sql": sql, "plan": data}


__all__ = [
    "execute_query",
    "execute_select",
    "execute_query_with_pagination",
    "explain_query",
]
