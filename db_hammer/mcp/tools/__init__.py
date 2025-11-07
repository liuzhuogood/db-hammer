"""MCP工具集合，导入时自动注册"""

from . import connection  # noqa: F401
from . import query  # noqa: F401
from . import crud  # noqa: F401
from . import export  # noqa: F401
from . import admin  # noqa: F401
from . import schema  # noqa: F401

__all__ = [
    "connection",
    "query",
    "crud",
    "export",
    "admin",
    "schema",
]
