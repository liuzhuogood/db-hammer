"""Table resources for MCP."""
from __future__ import annotations

try:  # pragma: no cover
    from fastmcp import resource
except ImportError:  # pragma: no cover
    def resource(path: str, **_kwargs):  # type: ignore
        def decorator(func):
            func.__mcp_resource__ = path
            return func

        return decorator

from ..tools.schema import list_tables, get_table_data


@resource("databases://{connection_id}/tables")
def list_table_resource_paths(connection_id: str):
    """Return table metadata URIs for the connection."""

    tables = list_tables(connection_id)
    return [
        {
            "uri": f"databases://{connection_id}/tables/{table['name']}/data",
            "table": table["name"],
        }
        for table in tables
    ]


@resource("databases://{connection_id}/tables/{table_name}/data")
def get_table_data_resource(connection_id: str, table_name: str):
    """Return preview data for a table."""

    rows = get_table_data(connection_id, table_name)
    return {"table": table_name, "rows": rows}


__all__ = ["list_table_resource_paths", "get_table_data_resource"]
