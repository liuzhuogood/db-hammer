from __future__ import annotations

from db_hammer.mcp.auth import AuthManager
from db_hammer.mcp.config import AuthenticationProvider
from db_hammer.mcp.exceptions import AuthenticationError


def test_auth_manager_api_key_success():
    manager = AuthManager([AuthenticationProvider(name="default", api_keys=["secret"])])
    assert manager.verify(api_key="secret") == "default"


def test_auth_manager_api_key_failure():
    manager = AuthManager([AuthenticationProvider(name="default", api_keys=["secret"])])
    try:
        manager.verify(api_key="bad")
    except AuthenticationError:
        pass
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected authentication error")
