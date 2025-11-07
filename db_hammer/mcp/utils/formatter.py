"""Utility functions for formatting database results."""
from __future__ import annotations

from typing import Iterable, List, Mapping


def format_rows(cursor_description: Iterable, rows: Iterable[Iterable]) -> List[Mapping[str, object]]:
    """Convert cursor results into a list of dictionaries."""

    columns = [col[0] for col in cursor_description]
    formatted = []
    for row in rows:
        formatted.append({col: value for col, value in zip(columns, row)})
    return formatted
