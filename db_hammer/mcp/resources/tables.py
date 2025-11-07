"""表结构资源"""
from __future__ import annotations

from ..exceptions import MCPServerError
from ..server import register_resource
from ..tools.schema import describe_table, list_tables


@register_resource("tables", "列出指定连接的所有表")
def tables_resource(params: dict):
    connection_id = params.get("connection_id")
    if not connection_id:
        raise MCPServerError("缺少connection_id参数")
    schema = params.get("schema")
    return list_tables(connection_id=connection_id, schema=schema)


@register_resource("table_columns", "查看指定表的字段信息")
def table_columns_resource(params: dict):
    connection_id = params.get("connection_id")
    table = params.get("table")
    if not connection_id or not table:
        raise MCPServerError("缺少必要参数")
    return describe_table(connection_id=connection_id, table=table)
