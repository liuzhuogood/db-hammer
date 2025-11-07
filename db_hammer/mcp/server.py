"""Entry point for db-hammer MCP server."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Tuple

from .fastmcp_adapter import FastMCP

from .config import MCPConfig, load_config
from .exceptions import MCPError
from .tools import connection as connection_tools
from .tools import crud as crud_tools
from .tools import export as export_tools
from .tools import query as query_tools
from .resources import connections as connections_resource
from .resources import schemas as schemas_resource
from .resources import tables as tables_resource

LOGGER = logging.getLogger(__name__)


def create_mcp_app(config_path: Optional[str] = None) -> Tuple[FastMCP, MCPConfig]:
    """Create and configure the FastMCP application."""

    config = load_config(config_path)
    LOGGER.info("Loaded MCP configuration: host=%s port=%s", config.host, config.port)

    app = FastMCP("db-hammer-mcp")

    connection_tools.register_tools(app)
    query_tools.register_tools(app)
    crud_tools.register_tools(app)
    export_manager = export_tools.ExportManager(config.storage_path)
    export_tools.register_tools(app, export_manager)

    tables_resource.register_resources(app)
    schemas_resource.register_resources(app)
    connections_resource.register_resources(app)

    return app, config


def run(config_path: Optional[str] = None) -> None:
    app, config = create_mcp_app(config_path)
    LOGGER.info("Starting MCP server on %s:%s", config.host, config.port)
    app.run(host=config.host, port=config.port)
