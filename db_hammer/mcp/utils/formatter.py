"""Helpers for formatting database results."""
from __future__ import annotations

from typing import Iterable, List, Sequence


def normalize_rows(rows: Iterable[Sequence]) -> List[List]:
    """Normalize database cursor rows into serialisable lists."""

    return [list(row) for row in rows]


def rows_to_dicts(columns: Sequence[str], rows: Iterable[Sequence]) -> List[dict]:
    """Convert rows into dictionaries based on provided column names."""

    normalised = []
    for row in rows:
        row_dict = {column: row[idx] for idx, column in enumerate(columns)}
        normalised.append(row_dict)
    return normalised


__all__ = ["normalize_rows", "rows_to_dicts"]
