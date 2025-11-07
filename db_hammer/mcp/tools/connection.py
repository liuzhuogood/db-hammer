"""Database connection tools."""

from __future__ import annotations

import importlib
import importlib.util
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..config import MCPConfig
from ..exceptions import ConnectionError
from . import registry


@dataclass
class ConnectionRecord:
    id: str
    driver: str
    handle: Any
    dsn: Dict[str, Any]
    config_id: Optional[str] = None


class ConnectionManager:
    def __init__(self, config: Optional[MCPConfig] = None) -> None:
        self._config = config
        self._connections: Dict[str, ConnectionRecord] = {}
        self._lock = threading.RLock()

    def _connect_sqlite(self, database: str, config_id: Optional[str] = None, **kwargs: Any) -> ConnectionRecord:
        conn = sqlite3.connect(database, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        record = ConnectionRecord(
            id=str(uuid.uuid4()),
            driver="sqlite",
            handle=conn,
            dsn={"database": database, **kwargs},
            config_id=config_id,
        )
        return record

    def _connect_using_module(
        self,
        module_name: str,
        connect_func: str,
        driver: str,
        config_id: Optional[str] = None,
        **kwargs: Any,
    ) -> ConnectionRecord:
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:  # pragma: no cover - depends on external drivers
            raise ConnectionError(f"Driver '{module_name}' is not installed") from exc
        connect = getattr(module, connect_func)
        conn = connect(**kwargs)
        return ConnectionRecord(
            id=str(uuid.uuid4()),
            driver=driver,
            handle=conn,
            dsn=kwargs,
            config_id=config_id,
        )

    def create_connection(self, db_type: str, config_id: Optional[str] = None, **kwargs: Any) -> str:
        db_type = db_type.lower()
        if db_type == "sqlite":
            if "database" not in kwargs and "database_path" not in kwargs:
                raise ConnectionError("'database' or 'database_path' is required for SQLite connections")
            database = kwargs.get("database") or kwargs.get("database_path")
            record = self._connect_sqlite(database, config_id=config_id)
        elif db_type == "mysql":
            record = self._connect_using_module("pymysql", "connect", "mysql", config_id=config_id, **kwargs)
        elif db_type == "postgresql":
            module_name = "psycopg2" if importlib.util.find_spec("psycopg2") else "psycopg"
            connect_func = "connect"
            record = self._connect_using_module(
                module_name, connect_func, "postgresql", config_id=config_id, **kwargs
            )
        elif db_type == "oracle":
            module_name = "oracledb"
            record = self._connect_using_module(
                module_name, "connect", "oracle", config_id=config_id, **kwargs
            )
        elif db_type == "mssql":
            module_name = "pymssql"
            record = self._connect_using_module(
                module_name, "connect", "mssql", config_id=config_id, **kwargs
            )
        else:
            raise ConnectionError(f"Unsupported database type: {db_type}")

        with self._lock:
            self._connections[record.id] = record
        return record.id

    def create_from_config(self, connection_id: str) -> str:
        if not self._config:
            raise ConnectionError("No configuration available")
        config = self._config.get_connection(connection_id)
        kwargs = {
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "password": config.password,
            "database": config.database,
        }
        kwargs.update(config.options)
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        if config.type == "oracle" and config.service_name:
            kwargs.setdefault("service_name", config.service_name)
        return self.create_connection(config.type, config_id=config.id, **kwargs)

    def get(self, connection_id: str) -> ConnectionRecord:
        try:
            return self._connections[connection_id]
        except KeyError as exc:
            raise ConnectionError(f"Connection '{connection_id}' does not exist") from exc

    def close(self, connection_id: str) -> bool:
        with self._lock:
            record = self._connections.pop(connection_id, None)
            if not record:
                return False
            try:
                record.handle.close()
            finally:
                return True

    def list(self) -> list:
        with self._lock:
            return [
                {
                    "id": record.id,
                    "driver": record.driver,
                    "dsn": record.dsn,
                    "config_id": record.config_id,
                }
                for record in self._connections.values()
            ]

    @property
    def has_config(self) -> bool:
        return self._config is not None

    @property
    def config(self) -> Optional[MCPConfig]:
        return self._config


_connection_manager: Optional[ConnectionManager] = None


def configure_manager(manager: ConnectionManager) -> None:
    global _connection_manager
    _connection_manager = manager


def _ensure_manager() -> ConnectionManager:
    if not _connection_manager:
        raise ConnectionError("Connection manager has not been configured")
    return _connection_manager


def get_manager() -> ConnectionManager:
    return _ensure_manager()


@registry.tool(name="create_mysql_connection", description="Create a MySQL connection")
def create_mysql_connection(host: str, user: str, password: str, database: str, port: int = 3306, **kwargs: Any) -> str:
    manager = _ensure_manager()
    return manager.create_connection(
        "mysql",
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        **kwargs,
    )


@registry.tool(name="create_postgresql_connection", description="Create a PostgreSQL connection")
def create_postgresql_connection(host: str, user: str, password: str, database: str, port: int = 5432, **kwargs: Any) -> str:
    manager = _ensure_manager()
    return manager.create_connection(
        "postgresql",
        host=host,
        user=user,
        password=password,
        dbname=database,
        port=port,
        **kwargs,
    )


@registry.tool(name="create_oracle_connection", description="Create an Oracle connection")
def create_oracle_connection(host: str, user: str, password: str, service_name: str, port: int = 1521, **kwargs: Any) -> str:
    manager = _ensure_manager()
    dsn = f"{host}:{port}/{service_name}"
    return manager.create_connection(
        "oracle",
        user=user,
        password=password,
        dsn=dsn,
        **kwargs,
    )


@registry.tool(name="create_mssql_connection", description="Create an MSSQL connection")
def create_mssql_connection(host: str, user: str, password: str, database: str, port: int = 1433, **kwargs: Any) -> str:
    manager = _ensure_manager()
    return manager.create_connection(
        "mssql",
        server=host,
        user=user,
        password=password,
        database=database,
        port=port,
        **kwargs,
    )


@registry.tool(name="create_sqlite_connection", description="Create a SQLite connection")
def create_sqlite_connection(database_path: str, **kwargs: Any) -> str:
    manager = _ensure_manager()
    return manager.create_connection("sqlite", database=database_path, **kwargs)


@registry.tool(name="close_connection", description="Close an existing connection")
def close_connection(connection_id: str) -> bool:
    manager = _ensure_manager()
    return manager.close(connection_id)


@registry.tool(name="list_connections", description="List active database connections")
def list_connections() -> list:
    manager = _ensure_manager()
    return manager.list()
