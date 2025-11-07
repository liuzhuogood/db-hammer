"""Validation helpers used by the MCP tools."""

from __future__ import annotations

import re
from typing import Iterable, Mapping, Sequence

from ..exceptions import QueryError

UNSAFE_SQL_PATTERN = re.compile(r";|--|/\*|\*/", re.IGNORECASE)


def ensure_sql_is_safe(sql: str) -> None:
    if not sql:
        raise QueryError("SQL statement cannot be empty")
    if UNSAFE_SQL_PATTERN.search(sql):
        raise QueryError("Potentially unsafe SQL detected")


def validate_columns(columns: Sequence[str] | None) -> Sequence[str] | None:
    if columns is None:
        return None
    if not all(columns):
        raise QueryError("Column names may not be empty")
    return columns


def validate_data_payload(data: Mapping[str, object]) -> Mapping[str, object]:
    if not data:
        raise QueryError("Payload cannot be empty")
    return data
