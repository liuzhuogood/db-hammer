"""连接资源"""
from __future__ import annotations

from ..server import get_tool_context, register_resource


@register_resource("connections", "当前活跃的数据库连接")
def list_connections(_: dict):
    context = get_tool_context()
    return context.registry.list_connections()
