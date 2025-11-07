"""Web configuration interface for the db-hammer MCP server."""
from .app import create_app
from .config_manager import WebConfigManager

__all__ = ["create_app", "WebConfigManager"]
