"""Web helpers for the db-hammer MCP server."""

from .config_manager import WebConfigManager

__all__ = ["create_app", "WebConfigManager"]


def create_app(*args, **kwargs):  # pragma: no cover - thin wrapper
    from .app import create_app as _create_app

    return _create_app(*args, **kwargs)
