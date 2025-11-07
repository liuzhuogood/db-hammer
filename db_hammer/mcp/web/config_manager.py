"""Web configuration manager for MCP server."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from ..config import MCPConfig, _dump_yaml, load_config


class ConfigManager:
    def __init__(self, path: str | None = None) -> None:
        self.path = Path(path or "./mcp_config.yaml")

    def load(self) -> MCPConfig:
        return load_config(str(self.path))

    def save(self, data: Dict[str, Any]) -> MCPConfig:
        config = MCPConfig.from_dict(data)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _dump_yaml(config.to_dict(), self.path)
        return config


def ensure_default_config(path: str | None = None) -> Path:
    target = Path(path or "./mcp_config.yaml")
    if target.exists():
        return target
    config = MCPConfig()
    target.parent.mkdir(parents=True, exist_ok=True)
    _dump_yaml(config.to_dict(), target)
    return target
