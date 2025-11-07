"""
 liuzhuogood@foxmail.com
 QQ:396687085
"""

DB_TYPE_MYSQL = "MYSQL"
DB_TYPE_ORACLE = "ORACLE"
DB_TYPE_POSTGRESQL = "POSTGRESQL"
DB_TYPE_SQLITE = "SQLITE"
DB_TYPE_MSSQL = "MSSQL"

try:  # pragma: no cover - optional module import
    from . import mcp  # noqa: F401
except Exception:  # pragma: no cover - allow installation without mcp extras
    mcp = None  # type: ignore

__all__ = [
    "DB_TYPE_MYSQL",
    "DB_TYPE_ORACLE",
    "DB_TYPE_POSTGRESQL",
    "DB_TYPE_SQLITE",
    "DB_TYPE_MSSQL",
    "mcp",
]
