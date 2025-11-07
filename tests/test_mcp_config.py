from pathlib import Path

import pytest

from db_hammer.mcp.config import ConfigurationError, load_config


def test_load_config_from_file(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text('{"host": "127.0.0.1", "port": 9000}', encoding="utf-8")
    config = load_config(str(config_path))
    assert config.host == "127.0.0.1"
    assert config.port == 9000


def test_load_config_missing_file(tmp_path: Path):
    with pytest.raises(ConfigurationError):
        load_config(str(tmp_path / "missing.yaml"))
