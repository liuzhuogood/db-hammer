"""MCP配置管理模块"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .utils.yaml_loader import safe_dump, safe_load


@dataclass
class ConnectionSettings:
    name: str
    type: str
    params: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    auto_connect: bool = False
    pool_size: int = 5

    def __post_init__(self) -> None:
        self.type = self.type.upper()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "params": dict(self.params),
            "description": self.description,
            "auto_connect": self.auto_connect,
            "pool_size": self.pool_size,
        }


@dataclass
class AuthSettings:
    api_keys: List[str] = field(default_factory=list)
    jwt_secret: str = "change-me"
    token_expire_seconds: int = 3600
    allow_anonymous: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "api_keys": list(self.api_keys),
            "jwt_secret": self.jwt_secret,
            "token_expire_seconds": self.token_expire_seconds,
            "allow_anonymous": self.allow_anonymous,
        }


@dataclass
class ExportSettings:
    storage_path: str = "./exports"
    auto_cleanup: bool = True
    retention_hours: int = 24

    def to_dict(self) -> Dict[str, Any]:
        return {
            "storage_path": self.storage_path,
            "auto_cleanup": self.auto_cleanup,
            "retention_hours": self.retention_hours,
        }


@dataclass
class AuditSettings:
    enabled: bool = True
    log_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"enabled": self.enabled, "log_path": self.log_path}


@dataclass
class MCPConfig:
    connections: Dict[str, ConnectionSettings] = field(default_factory=dict)
    default_connection: Optional[str] = None
    auth: AuthSettings = field(default_factory=AuthSettings)
    exports: ExportSettings = field(default_factory=ExportSettings)
    audit: AuditSettings = field(default_factory=AuditSettings)

    def validate(self) -> None:
        if self.default_connection and self.default_connection not in self.connections:
            raise ValueError(f"默认连接 {self.default_connection} 未在配置中定义")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connections": {name: settings.to_dict() for name, settings in self.connections.items()},
            "default_connection": self.default_connection,
            "auth": self.auth.to_dict(),
            "exports": self.exports.to_dict(),
            "audit": self.audit.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MCPConfig":
        connections_data = data.get("connections", {})
        connections = {
            name: ConnectionSettings(name=name, **{k: v for k, v in value.items() if k != "name"})
            for name, value in connections_data.items()
        }
        config = cls(
            connections=connections,
            default_connection=data.get("default_connection"),
            auth=AuthSettings(**data.get("auth", {})),
            exports=ExportSettings(**data.get("exports", {})),
            audit=AuditSettings(**data.get("audit", {})),
        )
        config.validate()
        return config


@dataclass
class ConfigManager:
    """配置管理器"""

    config_path: Optional[str] = None
    _config: MCPConfig = field(default_factory=MCPConfig)
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def __post_init__(self) -> None:
        if self.config_path is None:
            self.config_path = os.environ.get("DB_HAMMER_MCP_CONFIG", "mcp_config.yaml")
        self.config_path = str(Path(self.config_path).expanduser())
        self.reload()

    def reload(self) -> None:
        with self._lock:
            if os.path.exists(self.config_path):
                data = self._read_file(self.config_path)
            else:
                data = {}
            data = self._apply_env_overrides(data)
            self._config = MCPConfig.from_dict(data)
            self._ensure_storage_path()

    def save(self) -> None:
        with self._lock:
            path = Path(self.config_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(safe_dump(self._config.to_dict()), encoding="utf-8")

    def as_dict(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._config.to_dict()))

    def update(self, data: Dict[str, Any]) -> MCPConfig:
        with self._lock:
            merged = self.as_dict()
            self._deep_update(merged, data)
            self._config = MCPConfig.from_dict(merged)
            self._ensure_storage_path()
            return self._config

    def list_connection_names(self) -> List[str]:
        with self._lock:
            return list(self._config.connections.keys())

    def get_connection(self, name: str) -> ConnectionSettings:
        with self._lock:
            try:
                return self._config.connections[name]
            except KeyError as exc:
                raise KeyError(f"连接 {name} 未找到") from exc

    def set_connection(self, settings: ConnectionSettings) -> None:
        with self._lock:
            self._config.connections[settings.name] = settings
            if not self._config.default_connection:
                self._config.default_connection = settings.name
            self._ensure_storage_path()

    def delete_connection(self, name: str) -> None:
        with self._lock:
            if name in self._config.connections:
                del self._config.connections[name]
                if self._config.default_connection == name:
                    self._config.default_connection = None

    def list_api_keys(self) -> List[str]:
        with self._lock:
            return list(self._config.auth.api_keys)

    def add_api_key(self, api_key: str) -> None:
        with self._lock:
            if api_key not in self._config.auth.api_keys:
                self._config.auth.api_keys.append(api_key)

    def remove_api_key(self, api_key: str) -> None:
        with self._lock:
            self._config.auth.api_keys = [item for item in self._config.auth.api_keys if item != api_key]

    def get_exports_path(self) -> str:
        with self._lock:
            return self._config.exports.storage_path

    def iter_connections(self) -> Iterable[ConnectionSettings]:
        with self._lock:
            return list(self._config.connections.values())

    def _ensure_storage_path(self) -> None:
        path = Path(self._config.exports.storage_path).expanduser()
        path.mkdir(parents=True, exist_ok=True)

    def _read_file(self, path: str) -> Dict[str, Any]:
        text = Path(path).read_text(encoding="utf-8").strip()
        if not text:
            return {}
        if path.endswith(".json"):
            return json.loads(text)
        return safe_load(text)

    def _deep_update(self, original: Dict[str, Any], new_data: Dict[str, Any]) -> None:
        for key, value in new_data.items():
            if isinstance(value, dict) and isinstance(original.get(key), dict):
                self._deep_update(original[key], value)
            else:
                original[key] = value

    def _apply_env_overrides(self, data: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(data)
        api_keys = os.getenv("DB_HAMMER_MCP_API_KEYS")
        if api_keys:
            keys = [item.strip() for item in api_keys.split(",") if item.strip()]
            result.setdefault("auth", {})["api_keys"] = keys
        jwt_secret = os.getenv("DB_HAMMER_MCP_JWT_SECRET")
        if jwt_secret:
            result.setdefault("auth", {})["jwt_secret"] = jwt_secret
        default_conn = os.getenv("DB_HAMMER_MCP_DEFAULT_CONNECTION")
        if default_conn:
            result["default_connection"] = default_conn
        exports_path = os.getenv("DB_HAMMER_MCP_EXPORTS_PATH")
        if exports_path:
            result.setdefault("exports", {})["storage_path"] = exports_path
        return result


__all__ = [
    "ConfigManager",
    "MCPConfig",
    "ConnectionSettings",
]
