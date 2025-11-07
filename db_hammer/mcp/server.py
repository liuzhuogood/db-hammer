"""Entry point for running the db-hammer MCP server."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

try:  # pragma: no cover - optional runtime dependency
    from fastmcp import FastMCP
except Exception:  # pragma: no cover - executed when fastmcp is unavailable
    FastMCP = None  # type: ignore

from .auth import AuthManager
from .config import ConfigManager, MCPConfig
from .resources import connections, schemas, tables
from .resources import registry as resource_registry
from .tools import registry as tool_registry
from .tools.connection import ConnectionManager, configure_manager
from .tools.export import ExportManager, configure_export_manager
from .web import create_app

LOGGER = logging.getLogger(__name__)


class DBHammerMCPServer:
    """Facade that combines the MCP toolset, configuration, and web UI."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_manager = ConfigManager(config_path)
        self.config: MCPConfig = self.config_manager.load()
        self.connection_manager = ConnectionManager(self.config)
        configure_manager(self.connection_manager)
        self.export_manager = ExportManager(self.config.export.storage_path)
        configure_export_manager(self.export_manager)
        self.auth_manager = AuthManager(self.config.authentication)
        self._mcp_app = None
        self._flask_app = None

    # ------------------------------------------------------------------
    # MCP integration
    # ------------------------------------------------------------------
    def _wrap_tool(self, func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            LOGGER.debug("Executing tool %s", func.__name__)
            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper

    def build_fastmcp_app(self):  # pragma: no cover - depends on fastmcp
        if FastMCP is None:
            raise RuntimeError("fastmcp is not installed")
        app = FastMCP("db-hammer")
        for tool in tool_registry.items():
            decorator = app.tool(name=tool.name, description=tool.description)
            decorator(self._wrap_tool(tool.func))
        for resource in resource_registry.items():
            decorator = getattr(app, "resource", None)
            if decorator:
                decorator(name=resource.name, description=resource.description)(resource.loader)
        self._mcp_app = app
        return app

    # ------------------------------------------------------------------
    # HTTP integration
    # ------------------------------------------------------------------
    def build_flask_app(self):
        if self._flask_app is None:
            self._flask_app = create_app(
                self.config_manager.config_path,
                connection_manager=self.connection_manager,
                export_manager=self.export_manager,
            )
        return self._flask_app

    # ------------------------------------------------------------------
    def authenticate(self, api_key: Optional[str] = None, token: Optional[str] = None) -> str:
        return self.auth_manager.verify(api_key=api_key, token=token)

    def list_tools(self) -> Dict[str, str]:
        return {tool.name: tool.description or "" for tool in tool_registry.items()}

    def list_resources(self) -> Dict[str, str]:
        return {resource.name: resource.description or "" for resource in resource_registry.items()}

    def run(self, host: str = "127.0.0.1", port: int = 8000, use_http: bool = False) -> None:
        if not use_http and FastMCP is not None:
            LOGGER.info("Starting fastmcp server on stdio")
            app = self.build_fastmcp_app()
            app.run()  # type: ignore[attr-defined]
            return
        LOGGER.info("Starting HTTP server on http://%s:%s", host, port)
        app = self.build_flask_app()
        app.run(host=host, port=port)
