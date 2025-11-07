"""Schema resources for MCP."""
from __future__ import annotations

try:  # pragma: no cover
    from fastmcp import resource
except ImportError:  # pragma: no cover
    def resource(path: str, **_kwargs):  # type: ignore
        def decorator(func):
            func.__mcp_resource__ = path
            return func

        return decorator

from ..tools.schema import get_table_schema


@resource("databases://{connection_id}/tables/{table_name}/schema")
def get_schema_resource(connection_id: str, table_name: str):
    """Return schema metadata for a table."""

    return get_table_schema(connection_id, table_name)


__all__ = ["get_schema_resource"]
