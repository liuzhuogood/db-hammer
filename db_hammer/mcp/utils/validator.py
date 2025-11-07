"""Input validation helpers for the MCP server."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List

from ..exceptions import MCPError


@dataclass
class SQLValidator:
    """Very small SQL validation helper to mitigate basic injection attempts."""

    disallow_statements: Iterable[str] = (
        "drop",
        "truncate",
        "alter",
        "shutdown",
    )

    def ensure_safe_sql(self, sql: str) -> None:
        if not sql:
            raise MCPError("SQL语句不能为空")
        lowered = sql.lower()
        for statement in self.disallow_statements:
            pattern = rf"\b{re.escape(statement)}\b"
            if re.search(pattern, lowered):
                raise MCPError(f"SQL语句包含被禁止的指令: {statement}")

    def ensure_columns(self, columns: List[str]) -> None:
        if not columns:
            raise MCPError("列信息不能为空")
        for column in columns:
            if not re.match(r"^[a-zA-Z0-9_]+$", column):
                raise MCPError(f"列名包含非法字符: {column}")


__all__ = ["SQLValidator"]
