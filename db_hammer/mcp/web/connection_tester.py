"""Connection testing helpers for the web interface."""

from __future__ import annotations

from typing import Any, Dict

from ..exceptions import ConnectionError
from ..tools.connection import ConnectionManager


def test_connection(db_type: str, **kwargs: Any) -> Dict[str, Any]:
    manager = ConnectionManager()
    try:
        connection_id = manager.create_connection(db_type, **kwargs)
    except ConnectionError:
        raise
    except Exception as exc:  # pragma: no cover - driver specific
        raise ConnectionError(str(exc)) from exc
    finally:
        if "connection_id" in locals():
            manager.close(connection_id)
    return {"status": "ok"}
