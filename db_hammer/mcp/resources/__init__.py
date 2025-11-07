"""Resource registry for the MCP server."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional


@dataclass
class ResourceDefinition:
    name: str
    loader: Callable[[], Any]
    description: Optional[str] = None


class ResourceRegistry:
    def __init__(self) -> None:
        self._resources: Dict[str, ResourceDefinition] = {}

    def register(
        self,
        name: str,
        loader: Callable[[], Any],
        description: Optional[str] = None,
    ) -> None:
        self._resources[name] = ResourceDefinition(name=name, loader=loader, description=description)

    def items(self) -> Iterable[ResourceDefinition]:
        return list(self._resources.values())


registry = ResourceRegistry()
