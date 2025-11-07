"""Administrative tools for the MCP server."""

from __future__ import annotations

from typing import Any, Dict

from ..exceptions import ConnectionError
from . import registry
from .connection import get_manager
from .query import execute_query


@registry.tool(name="ping_database", description="Run a lightweight ping against the database")
def ping_database(connection_id: str) -> Dict[str, Any]:
    execute_query(connection_id, "SELECT 1")
    return {"connection_id": connection_id, "status": "ok"}


@registry.tool(name="refresh_connection", description="Refresh an existing connection from configuration")
def refresh_connection(connection_id: str) -> Dict[str, Any]:
    manager = get_manager()
    record = manager.get(connection_id)
    manager.close(connection_id)
    if not manager.has_config or not record.config_id:
        raise ConnectionError("No configuration available to recreate the connection")
    new_connection_id = manager.create_from_config(record.config_id)
    return {"old": connection_id, "new": new_connection_id}


@registry.tool(name="server_status", description="Get MCP server status")
def server_status() -> Dict[str, Any]:
    manager = get_manager()
    return {
        "connections": len(manager.list()),
    }
