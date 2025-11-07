"""Connection management tools for the MCP server."""
from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from typing import Dict

try:  # pragma: no cover - optional dependency fallback
    from fastmcp import tool
except ImportError:  # pragma: no cover
    def tool(func=None, **kwargs):  # type: ignore
        if func is None:
            return lambda wrapped: wrapped
        return func

from db_hammer.base import BaseConnection

from ..config import ConnectionConfig
from ..exceptions import ConnectionError


class ConnectionManager:
    """Thread-safe manager for active database connections."""

    def __init__(self):
        self._connections: Dict[str, BaseConnection] = {}
        self._lock = threading.RLock()

    def create_connection(self, config: ConnectionConfig) -> str:
        with self._lock:
            connection_cls = _resolve_connection_class(config.type)

            params = {
                "host": config.host,
                "port": config.port,
                "user": config.user,
                "password": config.password,
                "database": config.database,
            }
            if config.type == "oracle":
                params["service_name"] = config.service_name
            if config.type == "sqlite":
                params = {"database": config.database}
            params.update(config.options)

            connection = connection_cls(**{k: v for k, v in params.items() if v is not None})
            connection_id = f"{config.id}:{uuid.uuid4()}"
            self._connections[connection_id] = connection
            return connection_id

    def close_connection(self, connection_id: str) -> bool:
        with self._lock:
            connection = self._connections.pop(connection_id, None)
            if connection:
                connection.close()
                return True
            return False

    def list_connections(self) -> Dict[str, str]:
        with self._lock:
            return {conn_id: conn.db_type for conn_id, conn in self._connections.items()}

    def get(self, connection_id: str) -> BaseConnection:
        with self._lock:
            connection = self._connections.get(connection_id)
            if not connection:
                raise ConnectionError(f"Connection not found: {connection_id}")
            return connection

    @contextmanager
    def use(self, connection_id: str):
        connection = self.get(connection_id)
        try:
            yield connection
        finally:
            if connection.auto_commit:
                connection.conn.commit()


_MANAGER = ConnectionManager()


def _create_connection_from_params(connection_type: str, **params) -> str:
    config = ConnectionConfig(id=connection_type, type=connection_type, **params)
    return _MANAGER.create_connection(config)


def _resolve_connection_class(db_type: str):
    if db_type == "sqlite":
        from db_hammer.pysqlite3 import Sqlite3Connection

        return Sqlite3Connection
    if db_type == "mysql":
        from db_hammer.mysql import MySQLConnection

        return MySQLConnection
    if db_type == "postgresql":
        from db_hammer.postgresql import PostgreSQLConnection

        return PostgreSQLConnection
    if db_type == "oracle":
        from db_hammer.oracle import OracleConnection

        return OracleConnection
    if db_type == "mssql":
        from db_hammer.mssql import MsSQLConnection

        return MsSQLConnection
    raise ConnectionError(f"Unsupported database type: {db_type}")


@tool
def create_mysql_connection(host: str, user: str, password: str, database: str, port: int = 3306) -> str:
    """Create a MySQL connection and return the connection identifier."""

    return _create_connection_from_params(
        "mysql",
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@tool
def create_postgresql_connection(host: str, user: str, password: str, database: str, port: int = 5432) -> str:
    """Create a PostgreSQL connection and return the connection identifier."""

    return _create_connection_from_params(
        "postgresql",
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@tool
def create_oracle_connection(host: str, user: str, password: str, service_name: str, port: int = 1521) -> str:
    """Create an Oracle connection and return the connection identifier."""

    return _create_connection_from_params(
        "oracle",
        host=host,
        port=port,
        user=user,
        password=password,
        service_name=service_name,
    )


@tool
def create_mssql_connection(host: str, user: str, password: str, database: str, port: int = 1433) -> str:
    """Create an MSSQL connection and return the connection identifier."""

    return _create_connection_from_params(
        "mssql",
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@tool
def create_sqlite_connection(database_path: str) -> str:
    """Create a SQLite connection and return the connection identifier."""

    return _create_connection_from_params("sqlite", database=database_path)


@tool
def close_connection(connection_id: str) -> bool:
    """Close an existing connection."""

    return _MANAGER.close_connection(connection_id)


@tool
def list_connections() -> Dict[str, str]:
    """List current active connections."""

    return _MANAGER.list_connections()


def get_connection(connection_id: str) -> BaseConnection:
    """Return a connection by id."""

    return _MANAGER.get(connection_id)


def use_connection(connection_id: str):
    """Context manager helper for using a connection."""

    return _MANAGER.use(connection_id)


__all__ = [
    "ConnectionManager",
    "create_mysql_connection",
    "create_postgresql_connection",
    "create_oracle_connection",
    "create_mssql_connection",
    "create_sqlite_connection",
    "close_connection",
    "list_connections",
    "get_connection",
    "use_connection",
]
