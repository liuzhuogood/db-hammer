from typing import Any, List

from pydantic import BaseModel


class PageInput(BaseModel):
    sort_by: str = ""
    descending: bool = False
    page_size: int = 30
    page_start: int = 1


class PageOutput(BaseModel):
    rows_number: int = 0
    sort_by: str = ""
    descending: bool = False
    page_size: int = 30
    page_start: int = 1
    page_number: int = 1  # 页数
    rows: List[Any] = []
