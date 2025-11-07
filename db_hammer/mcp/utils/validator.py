"""参数校验工具"""
from __future__ import annotations

import re
from typing import Iterable, Sequence

from ..exceptions import ValidationError

FORBIDDEN_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bDROP\b",
        r"\bTRUNCATE\b",
        r"\bALTER\b",
        r"\bSHUTDOWN\b",
        r";",  # 禁止多语句执行
    ]
]

IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\.]*$")


def ensure_safe_sql(sql: str) -> None:
    if not sql or not sql.strip():
        raise ValidationError("SQL语句不能为空")
    for pattern in FORBIDDEN_PATTERNS:
        if pattern.search(sql):
            raise ValidationError("检测到潜在危险SQL语句")


def validate_table_name(table: str) -> None:
    if not IDENTIFIER_PATTERN.match(table):
        raise ValidationError(f"非法的表名: {table}")


def validate_columns(columns: Sequence[str]) -> None:
    for column in columns:
        if not IDENTIFIER_PATTERN.match(column):
            raise ValidationError(f"非法的字段名: {column}")


def ensure_required_fields(data: Iterable[str], payload: dict) -> None:
    for field in data:
        if field not in payload:
            raise ValidationError(f"缺少必要字段: {field}")


__all__ = [
    "ensure_safe_sql",
    "validate_table_name",
    "validate_columns",
    "ensure_required_fields",
]
