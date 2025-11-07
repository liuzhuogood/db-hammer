"""Connection resources for MCP."""
from __future__ import annotations

from typing import Dict, List

from ..fastmcp_adapter import FastMCP

from ..tools.connection import registry


def register_resources(app: FastMCP) -> None:
    @app.resource("connections")
    def connection_status() -> List[Dict[str, str]]:
        return [
            {"connection_id": connection_id, "db_type": db_type}
            for connection_id, db_type in registry.list_connections().items()
        ]
