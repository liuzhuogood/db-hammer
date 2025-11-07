"""MCP server configuration management without external dependencies."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore
except Exception:  # pragma: no cover - fallback path
    yaml = None

from .exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    name: str
    type: str
    dsn: Dict[str, Any] = field(default_factory=dict)
    pool_size: int = 5

    def __post_init__(self) -> None:
        allowed = {"mysql", "postgresql", "oracle", "mssql", "sqlite"}
        self.type = self.type.lower()
        if self.type not in allowed:
            raise ConfigurationError(f"Unsupported database type: {self.type}")
        if not isinstance(self.dsn, dict):
            raise ConfigurationError("Database DSN must be a dictionary")
        if self.pool_size < 1:
            raise ConfigurationError("pool_size must be >= 1")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "dsn": self.dsn,
            "pool_size": self.pool_size,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConfig":
        return cls(
            name=data.get("name", ""),
            type=data.get("type", ""),
            dsn=data.get("dsn", {}),
            pool_size=int(data.get("pool_size", 5)),
        )


@dataclass
class SecurityConfig:
    enable_token_auth: bool = True
    token_secret: Optional[str] = None
    allowed_origins: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enable_token_auth": self.enable_token_auth,
            "token_secret": self.token_secret,
            "allowed_origins": self.allowed_origins,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "SecurityConfig":
        data = data or {}
        return cls(
            enable_token_auth=bool(data.get("enable_token_auth", True)),
            token_secret=data.get("token_secret"),
            allowed_origins=list(data.get("allowed_origins", [])),
        )


@dataclass
class MCPConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    storage_path: str = "./exports"
    databases: List[DatabaseConfig] = field(default_factory=list)
    security: SecurityConfig = field(default_factory=SecurityConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "log_level": self.log_level,
            "storage_path": self.storage_path,
            "databases": [db.to_dict() for db in self.databases],
            "security": self.security.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MCPConfig":
        databases = [DatabaseConfig.from_dict(item) for item in data.get("databases", [])]
        security = SecurityConfig.from_dict(data.get("security"))
        return cls(
            host=data.get("host", "0.0.0.0"),
            port=int(data.get("port", 8000)),
            log_level=data.get("log_level", "INFO"),
            storage_path=data.get("storage_path", "./exports"),
            databases=databases,
            security=security,
        )


DEFAULT_CONFIG_PATHS = (
    Path("./mcp_config.yaml"),
    Path("~/.config/db_hammer/mcp_config.yaml").expanduser(),
)


def _load_yaml(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if yaml:
        return yaml.safe_load(text) or {}
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:  # pragma: no cover - fallback warning
        raise ConfigurationError(f"Unable to parse configuration file {path}: {exc}") from exc


def _dump_yaml(data: Dict[str, Any], path: Path) -> None:
    if yaml:
        yaml.safe_dump(data, path.open("w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
        return
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_config(path: Optional[str] = None) -> MCPConfig:
    config_data: Dict[str, Any] = {}
    config_path: Optional[Path] = None

    if path:
        candidate = Path(path)
        if candidate.exists():
            config_path = candidate
        else:
            raise ConfigurationError(f"Configuration file not found: {path}")
    else:
        for candidate in DEFAULT_CONFIG_PATHS:
            if candidate.exists():
                config_path = candidate
                break

    if config_path is not None:
        config_data = _load_yaml(config_path)

    prefix = "DB_HAMMER_MCP__"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        normalized = key[len(prefix) :].lower()
        if normalized == "token_secret":
            config_data.setdefault("security", {})
            config_data["security"]["token_secret"] = value
        elif normalized == "enable_token_auth":
            config_data.setdefault("security", {})
            config_data["security"]["enable_token_auth"] = value.lower() in {"1", "true", "yes"}
        elif normalized == "allowed_origins":
            config_data.setdefault("security", {})
            config_data["security"]["allowed_origins"] = [item.strip() for item in value.split(",") if item.strip()]
        elif normalized == "host":
            config_data["host"] = value
        elif normalized == "port":
            try:
                config_data["port"] = int(value)
            except ValueError as exc:  # pragma: no cover - defensive
                raise ConfigurationError("Environment variable port must be integer") from exc

    return MCPConfig.from_dict(config_data)


__all__ = ["load_config", "MCPConfig", "DatabaseConfig", "SecurityConfig", "_dump_yaml"]
