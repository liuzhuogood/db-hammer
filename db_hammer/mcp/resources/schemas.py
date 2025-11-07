"""Schema resources."""

from __future__ import annotations

from typing import Dict, List

from ..tools.connection import get_manager
from ..tools.schema import describe_table, list_tables
from . import registry


def _load_schemas() -> Dict[str, Dict[str, List[dict]]]:
    manager = get_manager()
    result: Dict[str, Dict[str, List[dict]]] = {}
    for connection in manager.list():
        connection_id = connection["id"]
        tables = list_tables(connection_id)
        table_map: Dict[str, List[dict]] = {}
        for entry in tables:
            table_name = entry.get("table_name") or entry.get("name") or entry.get("Tables_in_0")
            if not table_name:
                continue
            try:
                table_map[table_name] = describe_table(connection_id, table_name)
            except Exception as exc:  # pragma: no cover
                table_map[table_name] = [{"error": str(exc)}]
        result[connection_id] = table_map
    return result


registry.register("schemas", _load_schemas, description="Detailed schema information")
