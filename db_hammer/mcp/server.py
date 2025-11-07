"""Entry point for running the db-hammer MCP server."""
from __future__ import annotations

import logging
import inspect
from typing import Optional

try:  # pragma: no cover
    from fastmcp.server import FastMCPServer  # type: ignore
except ImportError:  # pragma: no cover
    FastMCPServer = None

from .config import MCPConfig, load_config
from .exceptions import MCPError
from .tools import connection, crud, export, query, schema
from .resources import connections, tables, schemas
from .utils.security import SecurityManager

LOGGER = logging.getLogger(__name__)


class SimpleMCPServer:
    """Fallback server used when fastmcp is unavailable."""

    def __init__(self, config: MCPConfig):
        self.config = config
        self.security = SecurityManager(config)
        self.tools = {}
        self.resources = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self._register_tool_module(connection)
        self._register_tool_module(query)
        self._register_tool_module(crud)
        self._register_tool_module(export)
        self._register_tool_module(schema)
        self._register_resource_module(connections)
        self._register_resource_module(tables)
        self._register_resource_module(schemas)

    def _register_tool_module(self, module) -> None:
        for attr in dir(module):
            if attr.startswith("_"):
                continue
            obj = getattr(module, attr)
            if inspect.isfunction(obj) and getattr(obj, "__module__", "") == module.__name__:
                self.tools[obj.__name__] = obj

    def _register_resource_module(self, module) -> None:
        for attr in dir(module):
            obj = getattr(module, attr)
            if callable(obj) and hasattr(obj, "__mcp_resource__"):
                self.resources[obj.__mcp_resource__] = obj

    def serve_forever(self) -> None:  # pragma: no cover - runtime usage
        if FastMCPServer is None:
            raise MCPError("fastmcp 库未安装，无法启动正式的MCP服务。请执行 `pip install db-hammer[mcp]`." )
        server = FastMCPServer("db-hammer", "db-hammer MCP Server")
        for func in self.tools.values():
            server.register_tool(func)
        for path, func in self.resources.items():
            server.register_resource(path, func)
        server.run(host=self.config.server.host, port=self.config.server.port)


def create_server(config: MCPConfig) -> SimpleMCPServer:
    """Create server instance for the provided configuration."""

    return SimpleMCPServer(config)


def run_server(config_path: Optional[str] = None) -> None:  # pragma: no cover - runtime usage
    """Load configuration and start the server."""

    config = load_config(config_path)
    server = create_server(config)
    LOGGER.info("启动 db-hammer MCP 服务，监听 %s:%s", config.server.host, config.server.port)
    server.serve_forever()


__all__ = ["create_server", "run_server", "SimpleMCPServer"]
