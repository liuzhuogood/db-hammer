"""Connection resources for MCP."""
from __future__ import annotations

from datetime import datetime

try:  # pragma: no cover
    from fastmcp import resource
except ImportError:  # pragma: no cover
    def resource(path: str, **_kwargs):  # type: ignore
        def decorator(func):
            func.__mcp_resource__ = path
            return func

        return decorator

from ..tools.connection import list_connections, get_connection


@resource("databases://connections")
def list_connection_resources():
    """Return active connection URIs."""

    connections = list_connections()
    return [
        {
            "uri": f"databases://{connection_id}/status",
            "connection_id": connection_id,
            "db_type": db_type,
        }
        for connection_id, db_type in connections.items()
    ]


@resource("databases://{connection_id}/status")
def get_connection_status(connection_id: str):
    """Return connection status metadata."""

    connection = get_connection(connection_id)
    return {
        "connection_id": connection_id,
        "db_type": connection.db_type,
        "description": f"连接 {connection_id}",
        "timestamp": datetime.utcnow().isoformat(),
    }


__all__ = ["list_connection_resources", "get_connection_status"]
