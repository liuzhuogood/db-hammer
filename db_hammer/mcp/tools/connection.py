"""MCP tools for managing database connections."""
from __future__ import annotations

import logging
import uuid
from typing import Dict

from ..fastmcp_adapter import FastMCP
from ..exceptions import ConnectionNotFoundError, MCPError

LOGGER = logging.getLogger(__name__)


def _optional_import(module_path: str, attribute: str):
    try:
        module = __import__(module_path, fromlist=[attribute])
        return getattr(module, attribute)
    except Exception:  # pragma: no cover - optional dependency
        LOGGER.info("Optional dependency %s.%s is unavailable", module_path, attribute)
        return None


MySQLConnection = _optional_import("db_hammer.mysql", "MySQLConnection")
PostgreSQLConnection = _optional_import("db_hammer.postgresql", "PostgreSQLConnection")
OracleConnection = _optional_import("db_hammer.oracle", "OracleConnection")
MsSQLConnection = _optional_import("db_hammer.mssql", "MsSQLConnection")
Sqlite3Connection = _optional_import("db_hammer.pysqlite3", "Sqlite3Connection")

from db_hammer import DB_TYPE_MYSQL, DB_TYPE_ORACLE, DB_TYPE_MSSQL, DB_TYPE_POSTGRESQL, DB_TYPE_SQLITE


class ConnectionRegistry:
    """In-memory registry storing active database connections."""

    def __init__(self) -> None:
        self._connections: Dict[str, object] = {}
        self._type_map = self._build_type_map()

    def _build_type_map(self) -> Dict[str, tuple[str, object]]:
        mapping: Dict[str, tuple[str, object]] = {}
        if MySQLConnection:
            mapping["mysql"] = (DB_TYPE_MYSQL, MySQLConnection)
        if PostgreSQLConnection:
            mapping["postgresql"] = (DB_TYPE_POSTGRESQL, PostgreSQLConnection)
        if OracleConnection:
            mapping["oracle"] = (DB_TYPE_ORACLE, OracleConnection)
        if MsSQLConnection:
            mapping["mssql"] = (DB_TYPE_MSSQL, MsSQLConnection)
        if Sqlite3Connection:
            mapping["sqlite"] = (DB_TYPE_SQLITE, Sqlite3Connection)
        return mapping

    def create_connection(self, db_type: str, **kwargs) -> str:
        db_type_lower = db_type.lower()
        if db_type_lower not in self._type_map:
            raise MCPError(f"Unsupported database type: {db_type}")

        _, conn_cls = self._type_map[db_type_lower]
        connection = conn_cls(**kwargs)
        connection_id = str(uuid.uuid4())
        self._connections[connection_id] = connection
        LOGGER.info("Created %s connection with id %s", db_type_lower, connection_id)
        return connection_id

    def get_connection(self, connection_id: str):
        try:
            return self._connections[connection_id]
        except KeyError as exc:
            raise ConnectionNotFoundError(f"Connection not found: {connection_id}") from exc

    def close_connection(self, connection_id: str) -> bool:
        connection = self._connections.pop(connection_id, None)
        if not connection:
            return False
        connection.close()
        LOGGER.info("Closed connection %s", connection_id)
        return True

    def list_connections(self) -> Dict[str, str]:
        return {connection_id: conn.db_type for connection_id, conn in self._connections.items()}


registry = ConnectionRegistry()


def register_tools(app: FastMCP) -> None:
    """Register connection management tools with the MCP application."""

    if "mysql" in registry._type_map:
        @app.tool()
        def create_mysql_connection(host: str, user: str, password: str, database: str, port: int = 3306) -> str:
            return registry.create_connection(
                "mysql", host=host, user=user, password=password, database=database, port=port
            )

    if "postgresql" in registry._type_map:
        @app.tool()
        def create_postgresql_connection(host: str, user: str, password: str, database: str, port: int = 5432) -> str:
            return registry.create_connection(
                "postgresql", host=host, user=user, password=password, database=database, port=port
            )

    if "oracle" in registry._type_map:
        @app.tool()
        def create_oracle_connection(host: str, user: str, password: str, service_name: str, port: int = 1521) -> str:
            dsn = f"{host}:{port}/{service_name}"
            return registry.create_connection("oracle", dsn=dsn, user=user, password=password)

    if "mssql" in registry._type_map:
        @app.tool()
        def create_mssql_connection(host: str, user: str, password: str, database: str, port: int = 1433) -> str:
            return registry.create_connection(
                "mssql", host=host, user=user, password=password, database=database, port=port
            )

    if "sqlite" in registry._type_map:
        @app.tool()
        def create_sqlite_connection(database_path: str) -> str:
            return registry.create_connection("sqlite", database=database_path)

    @app.tool()
    def close_connection(connection_id: str) -> bool:
        return registry.close_connection(connection_id)

    @app.tool()
    def list_connections() -> Dict[str, str]:
        return registry.list_connections()
