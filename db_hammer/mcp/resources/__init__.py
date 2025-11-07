"""MCP resources exposed by the server."""
from .connections import list_connection_resources
from .tables import list_table_resource_paths
from .schemas import get_schema_resource

__all__ = [
    "list_connection_resources",
    "list_table_resource_paths",
    "get_schema_resource",
]
