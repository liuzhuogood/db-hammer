"""连接测试工具"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from ..exceptions import MCPServerError

if TYPE_CHECKING:
    from ..tools.connection import ConnectionRegistry


def test_connection_params(registry: "ConnectionRegistry", payload: Dict[str, object]) -> Dict[str, object]:
    db_type = payload.get("type")
    params = payload.get("params")
    if not db_type or not isinstance(params, dict):
        raise MCPServerError("配置缺少type或params")
    created = registry.create_connection(db_type, **params)
    registry.close_connection(created["connection_id"])
    return {"status": "success"}
