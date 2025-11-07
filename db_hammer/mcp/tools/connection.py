"""连接管理工具"""
from __future__ import annotations

import importlib
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from db_hammer import (
    DB_TYPE_MSSQL,
    DB_TYPE_MYSQL,
    DB_TYPE_ORACLE,
    DB_TYPE_POSTGRESQL,
    DB_TYPE_SQLITE,
)

from ..config import ConfigManager
from ..exceptions import ConnectionNotFoundError, MCPServerError
from ..server import get_tool_context, mcp_tool
from ..utils.security import mask_connection_params

MODULE_MAP = {
    DB_TYPE_MYSQL: ("db_hammer.mysql", "MySQLConnection"),
    DB_TYPE_POSTGRESQL: ("db_hammer.postgresql", "PostgreSQLConnection"),
    DB_TYPE_ORACLE: ("db_hammer.oracle", "OracleConnection"),
    DB_TYPE_MSSQL: ("db_hammer.mssql", "MSSQLConnection"),
    DB_TYPE_SQLITE: ("db_hammer.pysqlite3", "Sqlite3Connection"),
}


@dataclass
class ConnectionInfo:
    connection_id: str
    db_type: str
    created_at: str
    params: Dict[str, Any]
    connection: Any


class ConnectionRegistry:
    def __init__(self, config_manager: ConfigManager) -> None:
        self.config_manager = config_manager
        self._connections: Dict[str, ConnectionInfo] = {}
        self._lock = threading.RLock()

    def create_connection(self, db_type: str, **params: Any) -> Dict[str, Any]:
        db_type = db_type.upper()
        module_name, class_name = MODULE_MAP.get(db_type, (None, None))
        if not module_name:
            raise MCPServerError(f"不支持的数据库类型: {db_type}")
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            raise MCPServerError(f"无法加载数据库驱动: {module_name}") from exc
        cls = getattr(module, class_name)
        connection = cls(**params)
        connection_id = str(uuid.uuid4())
        info = ConnectionInfo(
            connection_id=connection_id,
            db_type=db_type,
            created_at=datetime.now().isoformat(),
            params=params,
            connection=connection,
        )
        with self._lock:
            self._connections[connection_id] = info
        return {
            "connection_id": connection_id,
            "db_type": db_type,
            "created_at": info.created_at,
        }

    def create_from_config(self, name: str) -> Dict[str, Any]:
        settings = self.config_manager.get_connection(name)
        return self.create_connection(settings.type, **settings.params)

    def get_connection(self, connection_id: str):
        with self._lock:
            info = self._connections.get(connection_id)
        if not info:
            raise ConnectionNotFoundError(f"连接不存在: {connection_id}")
        return info.connection

    def close_connection(self, connection_id: str) -> bool:
        with self._lock:
            info = self._connections.pop(connection_id, None)
        if not info:
            return False
        try:
            info.connection.close()
        finally:
            return True

    def list_connections(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                connection_id: {
                    "db_type": info.db_type,
                    "created_at": info.created_at,
                    "params": mask_connection_params(info.params),
                }
                for connection_id, info in self._connections.items()
            }


def _create_connection(db_type: str, **params: Any) -> Dict[str, Any]:
    context = get_tool_context()
    return context.registry.create_connection(db_type, **params)


@mcp_tool(description="创建MySQL连接")
def create_mysql_connection(host: str, user: str, password: str, database: str, port: int = 3306, **kwargs) -> Dict[str, Any]:
    return _create_connection(DB_TYPE_MYSQL, host=host, user=user, password=password, database=database, port=port, **kwargs)


@mcp_tool(description="创建PostgreSQL连接")
def create_postgresql_connection(host: str, user: str, password: str, database: str, port: int = 5432, **kwargs) -> Dict[str, Any]:
    return _create_connection(DB_TYPE_POSTGRESQL, host=host, user=user, password=password, database=database, port=port, **kwargs)


@mcp_tool(description="创建Oracle连接")
def create_oracle_connection(host: str, user: str, password: str, service_name: str, port: int = 1521, **kwargs) -> Dict[str, Any]:
    params = dict(host=host, user=user, pwd=password, database=service_name, port=port, **kwargs)
    return _create_connection(DB_TYPE_ORACLE, **params)


@mcp_tool(description="创建MSSQL连接")
def create_mssql_connection(host: str, user: str, password: str, database: str, port: int = 1433, **kwargs) -> Dict[str, Any]:
    return _create_connection(DB_TYPE_MSSQL, host=host, user=user, password=password, database=database, port=port, **kwargs)


@mcp_tool(description="创建SQLite连接")
def create_sqlite_connection(database_path: str) -> Dict[str, Any]:
    return _create_connection(DB_TYPE_SQLITE, database=database_path)


@mcp_tool(description="关闭连接")
def close_connection(connection_id: str) -> bool:
    context = get_tool_context()
    return context.registry.close_connection(connection_id)


@mcp_tool(description="列出当前连接")
def list_connections() -> Dict[str, Dict[str, Any]]:
    context = get_tool_context()
    return context.registry.list_connections()


__all__ = [
    "ConnectionRegistry",
    "create_mysql_connection",
    "create_postgresql_connection",
    "create_oracle_connection",
    "create_mssql_connection",
    "create_sqlite_connection",
    "close_connection",
    "list_connections",
]
