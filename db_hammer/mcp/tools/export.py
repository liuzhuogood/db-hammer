"""Data export tools."""

from __future__ import annotations

import csv
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from ..exceptions import ExportError
from ..utils import ensure_sql_is_safe
from . import registry
from .connection import get_manager
from .query import execute_query


@dataclass
class ExportTask:
    export_id: str
    connection_id: str
    export_type: str
    params: Dict[str, Any]
    status: str = "pending"
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    file_path: Optional[str] = None
    file_size: int = 0
    download_url: Optional[str] = None
    error: Optional[str] = None
    completed_at: Optional[str] = None


class ExportManager:
    def __init__(self, storage_path: str = "./exports") -> None:
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.active_exports: Dict[str, ExportTask] = {}
        self._lock = threading.RLock()

    def create_export_task(self, connection_id: str, export_type: str, params: Dict[str, Any]) -> str:
        export_id = str(uuid.uuid4())
        task = ExportTask(
            export_id=export_id,
            connection_id=connection_id,
            export_type=export_type,
            params=params,
        )
        with self._lock:
            self.active_exports[export_id] = task
        return export_id

    def _write_csv(self, rows: Iterable[dict], path: Path) -> None:
        rows = list(rows)
        if not rows:
            path.write_text("", encoding="utf-8")
            return
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _export_table(self, task: ExportTask, file_path: Path) -> None:
        table = task.params["table"]
        where = task.params.get("where")
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        rows = execute_query(task.connection_id, sql)
        self._write_csv(rows, file_path)

    def _export_query(self, task: ExportTask, file_path: Path) -> None:
        sql = task.params["sql"]
        rows = execute_query(task.connection_id, sql)
        self._write_csv(rows, file_path)

    def _export_stream(self, task: ExportTask, file_path: Path) -> None:
        table = task.params["table"]
        batch_size = int(task.params.get("batch_size", 1000))
        connection_id = task.connection_id
        offset = 0
        all_rows = []
        while True:
            batch_sql = f"SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}"
            rows = execute_query(connection_id, batch_sql)
            if not rows:
                break
            all_rows.extend(rows)
            offset += batch_size
        self._write_csv(all_rows, file_path)

    def execute_export(self, export_id: str) -> Dict[str, Any]:
        with self._lock:
            task = self.active_exports.get(export_id)
        if not task:
            raise ExportError(f"Export task '{export_id}' does not exist")
        try:
            task.status = "running"
            file_name = f"{export_id}.{task.params.get('format', 'csv')}"
            file_path = self.storage_path / file_name
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if task.export_type == "table":
                self._export_table(task, file_path)
            elif task.export_type == "query":
                self._export_query(task, file_path)
            elif task.export_type == "stream":
                self._export_stream(task, file_path)
            else:
                raise ExportError(f"Unknown export type: {task.export_type}")
            task.status = "completed"
            task.file_path = str(file_path)
            task.file_size = file_path.stat().st_size
            task.download_url = f"/api/exports/download/{export_id}"
            task.completed_at = datetime.utcnow().isoformat()
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            raise
        return task.__dict__.copy()

    def get_export(self, export_id: str) -> ExportTask:
        with self._lock:
            task = self.active_exports.get(export_id)
            if not task:
                raise ExportError(f"Export task '{export_id}' not found")
            return task

    def delete_export(self, export_id: str) -> bool:
        with self._lock:
            task = self.active_exports.pop(export_id, None)
        if not task:
            return False
        if task.file_path and os.path.exists(task.file_path):
            os.remove(task.file_path)
        return True

    def list_exports(self) -> list:
        with self._lock:
            return [task.__dict__.copy() for task in self.active_exports.values()]


_export_manager: Optional[ExportManager] = None


def configure_export_manager(manager: ExportManager) -> None:
    global _export_manager
    _export_manager = manager


def _get_manager() -> ExportManager:
    if not _export_manager:
        raise ExportError("Export manager has not been configured")
    return _export_manager


@registry.tool(name="export_table_data", description="Export table data to a file")
def export_table_data(
    connection_id: str,
    table: str,
    format: str = "csv",
    where: Optional[str] = None,
) -> Dict[str, Any]:
    manager = _get_manager()
    export_id = manager.create_export_task(
        connection_id,
        "table",
        {"table": table, "format": format, "where": where},
    )
    return manager.execute_export(export_id)


@registry.tool(name="export_query_data", description="Export query results to a file")
def export_query_data(connection_id: str, sql: str, format: str = "csv") -> Dict[str, Any]:
    ensure_sql_is_safe(sql)
    manager = _get_manager()
    export_id = manager.create_export_task(
        connection_id,
        "query",
        {"sql": sql, "format": format},
    )
    return manager.execute_export(export_id)


@registry.tool(name="stream_large_table", description="Stream a large table to a file")
def stream_large_table(
    connection_id: str,
    table: str,
    batch_size: int = 1000,
    format: str = "csv",
) -> Dict[str, Any]:
    manager = _get_manager()
    export_id = manager.create_export_task(
        connection_id,
        "stream",
        {"table": table, "batch_size": batch_size, "format": format},
    )
    return manager.execute_export(export_id)


@registry.tool(name="get_export_status", description="Get the status of an export task")
def get_export_status(export_id: str) -> Dict[str, Any]:
    manager = _get_manager()
    return manager.get_export(export_id).__dict__.copy()


@registry.tool(name="download_export_file", description="Get the path of an exported file")
def download_export_file(export_id: str) -> str:
    manager = _get_manager()
    task = manager.get_export(export_id)
    if not task.file_path:
        raise ExportError("Export file not ready")
    return task.file_path


@registry.tool(name="list_export_files", description="List known export tasks")
def list_export_files() -> list:
    manager = _get_manager()
    return manager.list_exports()


@registry.tool(name="delete_export_file", description="Delete an exported file")
def delete_export_file(export_id: str) -> bool:
    manager = _get_manager()
    return manager.delete_export(export_id)
