"""Formatting utilities for MCP responses."""

from __future__ import annotations

import datetime as _dt
import decimal
from typing import Iterable, List, Mapping


def _convert_value(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    return value


def format_rows(columns: Iterable[str], rows: Iterable[Iterable]) -> List[dict]:
    columns = list(columns)
    formatted = []
    for row in rows:
        formatted.append({col: _convert_value(row[idx]) for idx, col in enumerate(columns)})
    return formatted


def serialize_exception(exc: Exception) -> dict:
    return {
        "type": exc.__class__.__name__,
        "message": str(exc),
    }
