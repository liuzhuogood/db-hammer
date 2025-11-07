"""Utilities that bridge :class:`~db_hammer.mcp.config.ConfigManager` with Flask."""

from __future__ import annotations

from typing import Any, Dict

from ..config import ConfigManager, MCPConfig


class WebConfigManager:
    def __init__(self, config_manager: ConfigManager | None = None) -> None:
        self._manager = config_manager or ConfigManager()

    @property
    def manager(self) -> ConfigManager:
        return self._manager

    def load(self) -> Dict[str, Any]:
        config = self.manager.load()
        data = config.to_dict()
        data.setdefault("metadata", {})
        data["metadata"]["config_path"] = str(self.manager.config_path)
        return data

    def save(self, data: Dict[str, Any]) -> MCPConfig:
        config = MCPConfig.parse_obj(data)
        self.manager.save(config)
        return config
