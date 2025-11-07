"""Table related MCP resources."""

from __future__ import annotations

from typing import Dict, List

from ..tools.connection import get_manager
from ..tools import schema as schema_tools
from . import registry


def _load_tables() -> Dict[str, List[dict]]:
    manager = get_manager()
    result: Dict[str, List[dict]] = {}
    for connection in manager.list():
        connection_id = connection["id"]
        try:
            result[connection_id] = schema_tools.list_tables(connection_id)
        except Exception as exc:  # pragma: no cover - driver specific
            result[connection_id] = [{"error": str(exc)}]
    return result


registry.register("tables", _load_tables, description="List tables for active connections")
