"""查询结果格式化工具"""
from __future__ import annotations

import datetime as _dt
import decimal
from typing import Any, Iterable, List


def normalize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def rows_to_dict_list(columns: Iterable[str], rows: Iterable[Iterable[Any]]) -> List[dict]:
    column_list = list(columns)
    results: List[dict] = []
    for row in rows:
        record = {}
        for index, column in enumerate(column_list):
            record[column] = normalize_value(row[index])
        results.append(record)
    return results


__all__ = ["normalize_value", "rows_to_dict_list"]
