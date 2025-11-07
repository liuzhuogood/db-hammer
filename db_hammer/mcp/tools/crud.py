"""数据操作工具"""
from __future__ import annotations

from typing import Dict, List

from ..server import get_tool_context, mcp_tool
from ..utils.validator import validate_columns, validate_table_name


def _execute(connection, sql: str, params: Dict[str, object]) -> int:
    sql, bound_params = connection.sql_params(sql, params)
    rowcount = connection.execute(sql, bound_params)
    if not connection.auto_commit:
        connection.conn.commit()
    return rowcount


@mcp_tool(description="插入数据")
def insert_data(connection_id: str, table: str, data: Dict[str, object]) -> Dict[str, int]:
    if not data:
        raise ValueError("data不能为空")
    validate_table_name(table)
    validate_columns(list(data.keys()))
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    columns = list(data.keys())
    placeholders = ", ".join([f":{col}" for col in columns])
    column_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"
    rowcount = _execute(connection, sql, data)
    return {"rowcount": rowcount}


@mcp_tool(description="更新数据")
def update_data(connection_id: str, table: str, data: Dict[str, object], where: str) -> Dict[str, int]:
    if not data:
        raise ValueError("data不能为空")
    validate_table_name(table)
    validate_columns(list(data.keys()))
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    set_clause = ", ".join([f"{column} = :{column}" for column in data])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
    rowcount = _execute(connection, sql, data)
    return {"rowcount": rowcount}


@mcp_tool(description="删除数据")
def delete_data(connection_id: str, table: str, where: str) -> Dict[str, int]:
    validate_table_name(table)
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    sql = f"DELETE FROM {table} WHERE {where}"
    rowcount = connection.execute(sql)
    if not connection.auto_commit:
        connection.conn.commit()
    return {"rowcount": rowcount}


@mcp_tool(description="Upsert数据")
def upsert_data(connection_id: str, table: str, data: Dict[str, object], conflict_columns: List[str]) -> Dict[str, int]:
    if not conflict_columns:
        raise ValueError("conflict_columns不能为空")
    validate_table_name(table)
    validate_columns(conflict_columns)
    validate_columns(list(data.keys()))
    context = get_tool_context()
    connection = context.registry.get_connection(connection_id)
    where_clause = " AND ".join([f"{col} = :{col}" for col in conflict_columns])
    select_sql = f"SELECT 1 FROM {table} WHERE {where_clause} LIMIT 1"
    sql, params = connection.sql_params(select_sql, data)
    connection.execute(sql, params)
    exists = connection.cursor.fetchone()
    if exists:
        set_columns = {key: value for key, value in data.items() if key not in conflict_columns}
        if not set_columns:
            return {"rowcount": 0}
        update_where = where_clause
        sql = f"UPDATE {table} SET " + ", ".join([f"{col} = :{col}" for col in set_columns]) + f" WHERE {update_where}"
        merged = {**data, **set_columns}
        rowcount = _execute(connection, sql, merged)
    else:
        rowcount = _execute(
            connection,
            f"INSERT INTO {table} ({', '.join(data.keys())}) VALUES ({', '.join([f':{col}' for col in data])})",
            data,
        )
    return {"rowcount": rowcount}


__all__ = ["insert_data", "update_data", "delete_data", "upsert_data"]
