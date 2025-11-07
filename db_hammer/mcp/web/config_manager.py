"""Web配置适配器"""
from __future__ import annotations

from typing import Any, Dict

from ..config import ConfigManager
from ..utils.security import mask_connection_params


class WebConfigManager:
    def __init__(self, manager: ConfigManager) -> None:
        self.manager = manager

    def get_config(self) -> Dict[str, Any]:
        config = self.manager.as_dict()
        masked_connections = {}
        for name, settings in config.get("connections", {}).items():
            params = settings.get("params", {})
            masked_connections[name] = {**settings, "params": mask_connection_params(params)}
        config["connections"] = masked_connections
        return config

    def update_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        config = self.manager.update(payload)
        return config.to_dict()


__all__ = ["WebConfigManager"]
