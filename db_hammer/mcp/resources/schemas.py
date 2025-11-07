"""Schema资源"""
from __future__ import annotations

from ..exceptions import MCPServerError
from ..server import register_resource
from ..tools.schema import list_schemas


@register_resource("schemas", "列出数据库Schema")
def schemas_resource(params: dict):
    connection_id = params.get("connection_id")
    if not connection_id:
        raise MCPServerError("缺少connection_id参数")
    return list_schemas(connection_id=connection_id)
