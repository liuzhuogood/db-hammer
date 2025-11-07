"""结构查询工具"""
from __future__ import annotations

from typing import Dict, List, Optional

from ..server import get_tool_context, mcp_tool
from ..utils.validator import validate_table_name


def _detect_db_type(connection) -> str:
    return getattr(connection, "db_type", "").upper()


@mcp_tool(description="列出数据表")
def list_tables(connection_id: str, schema: Optional[str] = None) -> Dict[str, List[str]]:
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = _detect_db_type(connection)
    if db_type == "SQLITE":
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        connection.execute(sql)
        tables = [row[0] for row in connection.cursor.fetchall()]
        return {"tables": tables}
    if db_type == "POSTGRESQL":
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema ORDER BY table_name"
        params = {"schema": schema or "public"}
    elif db_type == "MYSQL":
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema ORDER BY table_name"
        params = {"schema": schema or connection.database}
    else:
        sql = "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        params = {}
    sql, params = connection.sql_params(sql, params)
    connection.execute(sql, params)
    tables = [row[0] for row in connection.cursor.fetchall()]
    return {"tables": tables}


@mcp_tool(description="查看表结构")
def describe_table(connection_id: str, table: str) -> Dict[str, List[dict]]:
    validate_table_name(table)
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = _detect_db_type(connection)
    if db_type == "SQLITE":
        sql = f"PRAGMA table_info('{table}')"
        connection.execute(sql)
        columns = connection.cursor.fetchall()
        headers = ["cid", "name", "type", "notnull", "default_value", "pk"]
        return {"columns": [{headers[i]: row[i] for i in range(len(headers))} for row in columns]}
    sql = "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = :table"
    params = {"table": table}
    sql, params = connection.sql_params(sql, params)
    connection.execute(sql, params)
    columns = connection.cursor.fetchall()
    headers = ["column_name", "data_type", "is_nullable", "column_default"]
    return {"columns": [{headers[i]: row[i] for i in range(len(headers))} for row in columns]}


@mcp_tool(description="列出Schema")
def list_schemas(connection_id: str) -> Dict[str, List[str]]:
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = _detect_db_type(connection)
    if db_type == "SQLITE":
        return {"schemas": ["main"]}
    if db_type == "POSTGRESQL":
        sql = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
    elif db_type == "MYSQL":
        sql = "SHOW DATABASES"
    else:
        sql = "SELECT name FROM sys.schemas"
    connection.execute(sql)
    rows = connection.cursor.fetchall()
    schemas = [row[0] for row in rows]
    return {"schemas": schemas}


__all__ = ["list_tables", "describe_table", "list_schemas"]
