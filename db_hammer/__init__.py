"""
 liuzhuogood@foxmail.com
 QQ:396687085
"""

DB_TYPE_MYSQL = "MYSQL"
DB_TYPE_ORACLE = "ORACLE"
DB_TYPE_POSTGRESQL = "POSTGRESQL"
DB_TYPE_SQLITE = "SQLITE"
DB_TYPE_MSSQL = "MSSQL"

try:
    from .mcp import create_server, run_server
except Exception:  # pragma: no cover
    create_server = None
    run_server = None

__all__ = [
    "DB_TYPE_MYSQL",
    "DB_TYPE_ORACLE",
    "DB_TYPE_POSTGRESQL",
    "DB_TYPE_SQLITE",
    "DB_TYPE_MSSQL",
    "create_server",
    "run_server",
]
