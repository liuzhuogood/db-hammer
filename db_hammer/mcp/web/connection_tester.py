"""Utility for testing database connections via web interface."""
from __future__ import annotations

from dataclasses import dataclass

from ..tools.connection import ConnectionRegistry


@dataclass
class ConnectionTestResult:
    success: bool
    message: str


class ConnectionTester:
    def __init__(self) -> None:
        self.registry = ConnectionRegistry()

    def test(self, db_type: str, **kwargs) -> ConnectionTestResult:
        try:
            connection_id = self.registry.create_connection(db_type, **kwargs)
            self.registry.close_connection(connection_id)
            return ConnectionTestResult(True, "Connection successful")
        except Exception as exc:  # pragma: no cover - simple utility
            return ConnectionTestResult(False, str(exc))
