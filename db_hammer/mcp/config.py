"""Configuration management utilities for the db-hammer MCP server."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - optional dependency
    import yaml
except Exception:  # pragma: no cover - optional
    yaml = None  # type: ignore

from .exceptions import ConfigurationError


SUPPORTED_DATABASES = {"sqlite", "mysql", "postgresql", "oracle", "mssql"}


def _ensure_supported(db_type: str) -> str:
    db_type = db_type.lower()
    if db_type not in SUPPORTED_DATABASES:
        raise ConfigurationError(f"Unsupported database type: {db_type}")
    return db_type


@dataclass
class DatabaseConnectionConfig:
    id: str
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    service_name: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConnectionConfig":
        connection_id = data.get("id")
        if not connection_id:
            raise ConfigurationError("Connection id may not be empty")
        if any(ch.isspace() for ch in connection_id):
            raise ConfigurationError("Connection id may not contain whitespace")
        db_type = _ensure_supported(data.get("type", ""))
        options = dict(data.get("options") or {})
        return cls(
            id=connection_id,
            type=db_type,
            host=data.get("host"),
            port=data.get("port"),
            user=data.get("user"),
            password=data.get("password"),
            database=data.get("database"),
            service_name=data.get("service_name"),
            options=options,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "service_name": self.service_name,
            "options": self.options,
        }


@dataclass
class AuthenticationProvider:
    name: str
    api_keys: List[str] = field(default_factory=list)
    jwk: Optional[Dict[str, Any]] = None
    audience: Optional[str] = None
    issuer: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuthenticationProvider":
        name = data.get("name")
        if not name:
            raise ConfigurationError("Authentication provider name is required")
        api_keys = list(data.get("api_keys") or [])
        return cls(
            name=name,
            api_keys=api_keys,
            jwk=data.get("jwk"),
            audience=data.get("audience"),
            issuer=data.get("issuer"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "api_keys": self.api_keys,
            "jwk": self.jwk,
            "audience": self.audience,
            "issuer": self.issuer,
        }


@dataclass
class SecurityConfig:
    audit_log_path: Optional[str] = None
    require_tls: bool = False
    allowed_origins: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SecurityConfig":
        return cls(
            audit_log_path=data.get("audit_log_path"),
            require_tls=bool(data.get("require_tls", False)),
            allowed_origins=list(data.get("allowed_origins") or []),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "audit_log_path": self.audit_log_path,
            "require_tls": self.require_tls,
            "allowed_origins": self.allowed_origins,
        }


@dataclass
class ExportConfig:
    storage_path: str = "./exports"
    cleanup_after_hours: int = 24

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExportConfig":
        return cls(
            storage_path=data.get("storage_path", "./exports"),
            cleanup_after_hours=int(data.get("cleanup_after_hours", 24)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "storage_path": self.storage_path,
            "cleanup_after_hours": self.cleanup_after_hours,
        }


@dataclass
class MCPConfig:
    connections: List[DatabaseConnectionConfig] = field(default_factory=list)
    authentication: List[AuthenticationProvider] = field(default_factory=list)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "MCPConfig":
        data = data or {}
        connections = [DatabaseConnectionConfig.from_dict(item) for item in data.get("connections", [])]
        seen = set()
        for conn in connections:
            if conn.id in seen:
                raise ConfigurationError(f"Duplicate connection id detected: {conn.id}")
            seen.add(conn.id)
        authentication = [AuthenticationProvider.from_dict(item) for item in data.get("authentication", [])]
        security = SecurityConfig.from_dict(data.get("security") or {})
        export = ExportConfig.from_dict(data.get("export") or {})
        metadata = dict(data.get("metadata") or {})
        return cls(
            connections=connections,
            authentication=authentication,
            security=security,
            export=export,
            metadata=metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connections": [conn.to_dict() for conn in self.connections],
            "authentication": [provider.to_dict() for provider in self.authentication],
            "security": self.security.to_dict(),
            "export": self.export.to_dict(),
            "metadata": self.metadata,
        }

    def get_connection(self, connection_id: str) -> DatabaseConnectionConfig:
        for item in self.connections:
            if item.id == connection_id:
                return item
        raise ConfigurationError(f"Connection '{connection_id}' is not defined")


class ConfigManager:
    DEFAULT_LOCATIONS = (
        Path("./mcp_config.yaml"),
        Path.home() / ".db_hammer" / "mcp_config.yaml",
    )
    ENV_CONFIG_PATH = "DB_HAMMER_MCP_CONFIG"

    def __init__(self, config_path: Optional[str | Path] = None) -> None:
        self._config_path = Path(config_path) if config_path else None
        self._lock = RLock()
        self._cache: Optional[MCPConfig] = None

    @property
    def config_path(self) -> Path:
        if self._config_path is not None:
            return self._config_path
        env_path = os.getenv(self.ENV_CONFIG_PATH)
        if env_path:
            return Path(env_path)
        for path in self.DEFAULT_LOCATIONS:
            if path.exists():
                return path
        return self.DEFAULT_LOCATIONS[0]

    def load(self, force: bool = False) -> MCPConfig:
        with self._lock:
            if self._cache is not None and not force:
                return self._cache
            path = self.config_path
            if not path.exists():
                config = MCPConfig()
                self._cache = config
                return config
            try:
                with path.open("r", encoding="utf-8") as handle:
                    if path.suffix in {".yaml", ".yml"}:
                        if yaml is None:
                            raise ConfigurationError("PyYAML is required to read YAML configuration files")
                        data = yaml.safe_load(handle)
                    else:
                        data = json.load(handle)
            except OSError as exc:  # pragma: no cover - unlikely
                raise ConfigurationError(f"Unable to read configuration file: {exc}") from exc
            except (json.JSONDecodeError, Exception) as exc:
                raise ConfigurationError(f"Configuration file is invalid: {exc}") from exc

            config = MCPConfig.from_dict(data)
            self._cache = config
            return config

    def save(self, config: MCPConfig) -> None:
        with self._lock:
            path = self.config_path
            path.parent.mkdir(parents=True, exist_ok=True)
            data = config.to_dict()
            if path.suffix in {".yaml", ".yml", ""}:
                if yaml is None:
                    raise ConfigurationError("PyYAML is required to write YAML configuration files")
                with path.open("w", encoding="utf-8") as handle:
                    yaml.safe_dump(data, handle, allow_unicode=True, sort_keys=False)
            else:
                with path.open("w", encoding="utf-8") as handle:
                    json.dump(data, handle, indent=2)
            self._cache = config

    def update(self, **kwargs: Any) -> MCPConfig:
        config = self.load()
        data = config.to_dict()
        data.update(kwargs)
        updated = MCPConfig.from_dict(data)
        self.save(updated)
        return updated


def load_config(config_path: Optional[str | Path] = None) -> MCPConfig:
    return ConfigManager(config_path=config_path).load()
