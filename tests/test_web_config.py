from db_hammer.mcp.web.config_manager import ConfigManager
from db_hammer.mcp.web.connection_tester import ConnectionTester


def test_config_manager_roundtrip(tmp_path):
    path = tmp_path / "config.yaml"
    manager = ConfigManager(str(path))
    manager.save({"host": "0.0.0.0", "port": 9001, "storage_path": "./data", "databases": []})
    loaded = manager.load()
    assert loaded.port == 9001


def test_connection_tester(tmp_path):
    tester = ConnectionTester()
    result = tester.test("sqlite", database=str(tmp_path / "test.sqlite"))
    assert result.success
