"""Schema resources for MCP."""
from __future__ import annotations

from typing import Dict, List

from ..fastmcp_adapter import FastMCP

from ..tools.connection import registry


def register_resources(app: FastMCP) -> None:
    @app.resource("schema")
    def describe_table(connection_id: str, table: str) -> List[Dict[str, object]]:
        connection = registry.get_connection(connection_id)
        db_type = str(getattr(connection, "db_type", "")).lower()
        if db_type == "sqlite":
            sql = f"PRAGMA table_info({table})"
            rows = connection.select_dict_list(sql)
            return rows
        sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'"
        return connection.select_dict_list(sql)
