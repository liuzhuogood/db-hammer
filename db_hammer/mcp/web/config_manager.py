"""Configuration utilities for the web UI."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from ..config import MCPConfig, load_config, save_config
from .connection_tester import test_connections_async


class WebConfigManager:
    """Manage MCP configuration for the web interface."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else None

    def load(self) -> MCPConfig:
        path = str(self.config_path) if self.config_path else None
        return load_config(path)

    def save(self, config_data: Dict) -> MCPConfig:
        config = MCPConfig.from_dict(config_data)
        path = str(self.config_path) if self.config_path else None
        save_config(config, path)
        return config

    def test_connections(self, connections: List[Dict[str, object]]) -> List[Dict[str, object]]:
        return test_connections_async(connections)

    def generate_default(self) -> MCPConfig:
        config = MCPConfig()
        path = str(self.config_path) if self.config_path else None
        save_config(config, path)
        return config


__all__ = ["WebConfigManager"]
