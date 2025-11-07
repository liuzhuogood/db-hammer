"""Connection resources."""

from __future__ import annotations

from ..tools.connection import get_manager
from . import registry


def _load_connections() -> list:
    manager = get_manager()
    return manager.list()


registry.register("connections", _load_connections, description="Active connection overview")
