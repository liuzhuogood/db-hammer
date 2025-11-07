"""Simple validation helpers."""
from __future__ import annotations

from typing import Iterable

from ..exceptions import MCPError


def require_fields(data: dict, fields: Iterable[str]) -> None:
    for field in fields:
        if field not in data or data[field] is None:
            raise MCPError(f"Missing required field: {field}")


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise MCPError(message)
