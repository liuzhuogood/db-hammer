"""MCP integration package for db-hammer.

This module exposes helpers to run the db-hammer MCP server and to access
configuration utilities programmatically.  The implementation is intentionally
lightweight so that it can be used both in a real MCP context (when the
``fastmcp`` package is available) and in unit tests where the dependency may not
be installed.  The public API mirrors the functionality described in
``MCP_SERVER_PLAN.md`` and provides sensible fallbacks when optional
dependencies are missing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from .config import MCPConfig, ConfigManager
from .server import DBHammerMCPServer

__all__ = [
    "MCPConfig",
    "ConfigManager",
    "DBHammerMCPServer",
    "load_config",
]


def load_config(config_path: Optional[str | Path] = None) -> MCPConfig:
    """Convenience helper that mirrors :meth:`ConfigManager.load`.

    Parameters
    ----------
    config_path:
        Optional path to the configuration file.  When omitted the manager will
        resolve the location using the environment variables defined by the
        configuration subsystem.
    """

    manager = ConfigManager(config_path=config_path)
    return manager.load()
