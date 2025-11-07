from __future__ import annotations

import json
from pathlib import Path

from db_hammer.mcp.config import ConfigManager, MCPConfig


def test_config_manager_loads_default(tmp_path):
    config_path = tmp_path / "config.yaml"
    manager = ConfigManager(config_path=config_path)
    config = manager.load()
    assert isinstance(config, MCPConfig)
    assert config.connections == []


def test_config_manager_save_and_reload(tmp_path):
    config_path = tmp_path / "config.json"
    manager = ConfigManager(config_path=config_path)
    config = MCPConfig.from_dict(
        {
            "connections": [
                {
                    "id": "sqlite",
                    "type": "sqlite",
                    "database": str(tmp_path / "db.sqlite"),
                }
            ],
            "authentication": [],
        }
    )
    manager.save(config)
    reloaded = manager.load(force=True)
    assert reloaded.connections[0].id == "sqlite"
