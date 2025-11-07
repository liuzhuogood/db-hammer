"""Configuration management for the db-hammer MCP server."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional

try:  # pragma: no cover
    import yaml
except ImportError:  # pragma: no cover
    class _MiniYAML:
        @staticmethod
        def safe_load(stream):
            if hasattr(stream, "read"):
                return json.loads(stream.read() or "{}")
            return json.loads(stream or "{}")

        @staticmethod
        def safe_dump(data, fh, allow_unicode=True, sort_keys=False):
            json.dump(data, fh, indent=2, ensure_ascii=not allow_unicode)

    yaml = _MiniYAML()  # type: ignore

from .exceptions import ConfigurationError

DEFAULT_CONFIG_FILENAME = "mcp_config.yaml"
ENV_CONFIG_PATH = "DB_HAMMER_MCP_CONFIG"


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8800
    debug: bool = False
    web_enabled: bool = True
    web_port: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ServerConfig":
        known = {k: data.get(k, getattr(cls(), k)) for k in ["host", "port", "debug", "web_enabled", "web_port"]}
        host = known["host"]
        if not isinstance(host, str) or not host:
            raise ConfigurationError("服务器host配置无效")
        port = int(known["port"])
        if port < 1 or port > 65535:
            raise ConfigurationError("服务器端口必须在1-65535之间")
        return cls(host=host, port=port, debug=bool(known["debug"]), web_enabled=bool(known["web_enabled"]), web_port=known["web_port"])

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class SecurityProvider:
    provider: str
    options: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "SecurityProvider":
        provider = data.get("provider")
        if not isinstance(provider, str) or not provider:
            raise ConfigurationError("安全提供商配置无效")
        options = data.get("options") or {}
        if not isinstance(options, dict):
            raise ConfigurationError("安全提供商参数必须为字典")
        return cls(provider=provider, options={str(k): str(v) for k, v in options.items()})

    def to_dict(self) -> Dict[str, object]:
        return {"provider": self.provider, "options": dict(self.options)}


@dataclass
class SecurityConfig:
    enabled: bool = True
    providers: List[SecurityProvider] = field(default_factory=list)
    default_api_keys: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "SecurityConfig":
        providers_data = data.get("providers", []) or []
        providers = [SecurityProvider.from_dict(item) for item in providers_data]
        api_keys = data.get("default_api_keys", []) or []
        return cls(enabled=bool(data.get("enabled", True)), providers=providers, default_api_keys=[str(key) for key in api_keys])

    def to_dict(self) -> Dict[str, object]:
        return {
            "enabled": self.enabled,
            "providers": [provider.to_dict() for provider in self.providers],
            "default_api_keys": list(self.default_api_keys),
        }


@dataclass
class ConnectionConfig:
    id: str
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    service_name: Optional[str] = None
    name: Optional[str] = None
    options: Dict[str, str] = field(default_factory=dict)
    read_only: bool = False

    def __post_init__(self) -> None:
        supported = {"mysql", "postgresql", "oracle", "mssql", "sqlite"}
        if self.type not in supported:
            raise ConfigurationError(f"Unsupported database type: {self.type}")
        if not self.id:
            raise ConfigurationError("Connection id cannot be empty")

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ConnectionConfig":
        options = data.get("options", {}) or {}
        if not isinstance(options, dict):
            raise ConfigurationError("连接参数options必须是字典")
        return cls(
            id=str(data.get("id", "")),
            type=str(data.get("type", "")),
            host=data.get("host"),
            port=int(data["port"]) if data.get("port") is not None else None,
            user=data.get("user"),
            password=data.get("password"),
            database=data.get("database"),
            service_name=data.get("service_name"),
            name=data.get("name"),
            options={str(k): str(v) for k, v in options.items()},
            read_only=bool(data.get("read_only", False)),
        )

    def to_dict(self) -> Dict[str, object]:
        result = asdict(self)
        return result


@dataclass
class MCPConfig:
    server: ServerConfig = field(default_factory=ServerConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    connections: List[ConnectionConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "MCPConfig":
        server = ServerConfig.from_dict(data.get("server", {}) or {})
        security = SecurityConfig.from_dict(data.get("security", {}) or {})
        connections_data = data.get("connections", []) or []
        connections = [ConnectionConfig.from_dict(item) for item in connections_data]
        ids = [conn.id for conn in connections]
        if len(ids) != len(set(ids)):
            raise ConfigurationError("Connection ids must be unique")
        return cls(server=server, security=security, connections=connections)

    def to_dict(self) -> Dict[str, object]:
        return {
            "server": self.server.to_dict(),
            "security": self.security.to_dict(),
            "connections": [connection.to_dict() for connection in self.connections],
        }

    def get_connection(self, connection_id: str) -> ConnectionConfig:
        for connection in self.connections:
            if connection.id == connection_id:
                return connection
        raise ConfigurationError(f"Connection configuration not found: {connection_id}")


def get_default_config_path() -> Path:
    return Path(os.getenv(ENV_CONFIG_PATH, DEFAULT_CONFIG_FILENAME)).expanduser().resolve()


def load_config(path: Optional[os.PathLike] = None) -> MCPConfig:
    config_path = Path(path) if path else get_default_config_path()
    if not config_path.exists():
        raise ConfigurationError(f"Configuration file not found: {config_path}")
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except Exception as exc:
        raise ConfigurationError(f"Invalid configuration: {exc}") from exc
    return MCPConfig.from_dict(data)


def save_config(config: MCPConfig, path: Optional[os.PathLike] = None) -> Path:
    config_path = Path(path) if path else get_default_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(config.to_dict(), fh, allow_unicode=True, sort_keys=False)
    return config_path


__all__ = [
    "MCPConfig",
    "ServerConfig",
    "SecurityConfig",
    "SecurityProvider",
    "ConnectionConfig",
    "load_config",
    "save_config",
    "get_default_config_path",
]
