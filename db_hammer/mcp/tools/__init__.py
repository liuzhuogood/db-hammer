"""Tool registration infrastructure."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional


@dataclass
class ToolDefinition:
    name: str
    func: Callable[..., Any]
    description: Optional[str] = None


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: "OrderedDict[str, ToolDefinition]" = OrderedDict()

    def tool(self, name: Optional[str] = None, description: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            key = name or func.__name__
            self._tools[key] = ToolDefinition(name=key, func=func, description=description)
            return func

        return decorator

    def register(self, name: str, func: Callable[..., Any], description: Optional[str] = None) -> None:
        self._tools[name] = ToolDefinition(name=name, func=func, description=description)

    def items(self) -> Iterable[ToolDefinition]:
        return list(self._tools.values())

    def __contains__(self, item: str) -> bool:  # pragma: no cover - convenience
        return item in self._tools


registry = ToolRegistry()
