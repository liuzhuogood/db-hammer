"""Adapter to provide a minimal FastMCP implementation when the real package is unavailable."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict

try:  # pragma: no cover - only executed when fastmcp available
    from fastmcp import FastMCP as FastMCPBase  # type: ignore
except Exception:  # pragma: no cover - fallback path
    FastMCPBase = None

LOGGER = logging.getLogger(__name__)


class _ToolRegistry:
    def __init__(self) -> None:
        self.tools: Dict[str, Callable[..., Any]] = {}
        self.resources: Dict[str, Callable[..., Any]] = {}

    def register_tool(self, name: str, func: Callable[..., Any]) -> None:
        LOGGER.debug("Registered MCP tool: %s", name)
        self.tools[name] = func

    def register_resource(self, name: str, func: Callable[..., Any]) -> None:
        LOGGER.debug("Registered MCP resource: %s", name)
        self.resources[name] = func


class _FallbackFastMCP:
    def __init__(self, name: str) -> None:
        self.name = name
        self.registry = _ToolRegistry()

    def tool(self, name: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.registry.register_tool(name or func.__name__, func)
            return func

        return decorator

    def resource(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.registry.register_resource(name, func)
            return func

        return decorator

    def run(self, host: str = "0.0.0.0", port: int = 8000) -> None:  # pragma: no cover - debug utility
        LOGGER.info("Fallback FastMCP server running at http://%s:%s (no-op)", host, port)


FastMCP = FastMCPBase or _FallbackFastMCP
