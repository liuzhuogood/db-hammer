"""MCP resources for table listings."""
from __future__ import annotations

from typing import Dict, List

from ..fastmcp_adapter import FastMCP

from ..tools.connection import registry


def register_resources(app: FastMCP) -> None:
    @app.resource("tables")
    def list_tables(connection_id: str) -> List[Dict[str, str]]:
        connection = registry.get_connection(connection_id)
        db_type = str(getattr(connection, "db_type", "")).lower()
        if db_type == "sqlite":
            sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            rows = connection.select_dict_list(sql)
            return rows
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
        return connection.select_dict_list(sql)
