"""数据库管理工具"""
from __future__ import annotations

from typing import Dict

from ..server import get_tool_context, mcp_tool


@mcp_tool(description="连接探活")
def ping_database(connection_id: str) -> Dict[str, object]:
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    connection.execute("SELECT 1")
    result = connection.cursor.fetchone()
    return {"status": "ok", "result": result[0] if result else None}


@mcp_tool(description="获取数据库版本")
def get_database_version(connection_id: str) -> Dict[str, object]:
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = getattr(connection, "db_type", "").upper()
    if db_type == "SQLITE":
        version_sql = "SELECT sqlite_version()"
    else:
        version_sql = "SELECT version()"
    connection.execute(version_sql)
    version = connection.cursor.fetchone()
    return {"db_type": db_type, "version": version[0] if version else None}


@mcp_tool(description="获取服务器时间")
def get_server_time(connection_id: str) -> Dict[str, object]:
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = getattr(connection, "db_type", "").upper()
    if db_type == "SQLITE":
        sql = "SELECT datetime('now')"
    else:
        sql = "SELECT CURRENT_TIMESTAMP"
    connection.execute(sql)
    value = connection.cursor.fetchone()
    return {"server_time": value[0] if value else None}


@mcp_tool(description="执行维护操作")
def run_maintenance(connection_id: str) -> Dict[str, object]:
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    db_type = getattr(connection, "db_type", "").upper()
    if db_type == "SQLITE":
        connection.execute("VACUUM")
        connection.execute("ANALYZE")
    else:
        connection.execute("SELECT 1")
    if not connection.auto_commit:
        connection.conn.commit()
    return {"status": "maintenance_completed"}


__all__ = ["ping_database", "get_database_version", "get_server_time", "run_maintenance"]
