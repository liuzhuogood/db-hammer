"""Helpers for testing database connections from the web UI."""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from ..config import ConnectionConfig
from ..tools.connection import ConnectionManager


def _create_connection_from_config(config_dict: Dict[str, object]) -> str:
    manager = ConnectionManager()
    config = ConnectionConfig.from_dict(config_dict)
    connection_id = manager.create_connection(config)
    manager.close_connection(connection_id)
    return connection_id


def test_single_connection(connection_config: Dict[str, object]) -> Dict[str, object]:
    start_time = time.time()
    try:
        _create_connection_from_config(connection_config)
        status = "success"
        message = "连接成功"
    except Exception as exc:  # pragma: no cover - depends on DB state
        status = "failed"
        message = f"连接失败: {exc}"
    duration = round(time.time() - start_time, 3)
    return {
        "connection_id": connection_config.get("id", "unknown"),
        "status": status,
        "duration": duration,
        "message": message,
    }


def test_connections_async(connections: List[Dict[str, object]], timeout: int = 5) -> List[Dict[str, object]]:
    if not connections:
        return []
    results: List[Dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=min(len(connections), 5)) as executor:
        future_map = {executor.submit(test_single_connection, conn): conn for conn in connections}
        for future in as_completed(future_map, timeout=timeout):
            try:
                results.append(future.result(timeout=timeout))
            except Exception:  # pragma: no cover
                conn = future_map[future]
                results.append(
                    {
                        "connection_id": conn.get("id", "unknown"),
                        "status": "timeout",
                        "duration": timeout,
                        "message": "连接超时",
                    }
                )
    return results


__all__ = ["test_single_connection", "test_connections_async"]
