"""导出任务管理"""
from __future__ import annotations

import csv
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .exceptions import ExportTaskError
from .utils.formatter import normalize_value
from .utils.security import ensure_directory


@dataclass
class ExportTask:
    export_id: str
    connection_id: str
    export_type: str
    params: Dict[str, Any]
    status: str = "pending"
    file_path: Optional[str] = None
    file_size: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    download_url: Optional[str] = None
    error: Optional[str] = None


class ExportManager:
    """管理导出任务生命周期"""

    def __init__(self, storage_path: str = "./exports") -> None:
        self.storage_path = storage_path
        ensure_directory(self.storage_path)
        self.active_exports: Dict[str, ExportTask] = {}
        self._lock = threading.RLock()

    def create_export_task(self, connection_id: str, export_type: str, params: Dict[str, Any]) -> str:
        export_id = str(uuid.uuid4())
        task = ExportTask(export_id=export_id, connection_id=connection_id, export_type=export_type, params=params)
        with self._lock:
            self.active_exports[export_id] = task
        return export_id

    def execute_export(self, export_id: str, connection_registry) -> Dict[str, Any]:
        with self._lock:
            task = self.active_exports.get(export_id)
        if not task:
            raise ExportTaskError(f"导出任务不存在: {export_id}")
        try:
            task.status = "running"
            file_ext = task.params.get("format", "csv")
            file_name = f"{export_id}.{file_ext}"
            file_path = os.path.join(self.storage_path, file_name)
            ensure_directory(self.storage_path)
            if task.export_type == "table":
                self._export_table(connection_registry, task, file_path)
            elif task.export_type == "query":
                self._export_query(connection_registry, task, file_path)
            elif task.export_type == "stream":
                self._export_stream(connection_registry, task, file_path)
            else:
                raise ExportTaskError(f"未知的导出类型: {task.export_type}")
            task.status = "completed"
            task.file_path = file_path
            task.file_size = os.path.getsize(file_path)
            task.download_url = f"/api/exports/download/{export_id}"
            task.completed_at = datetime.now().isoformat()
        except Exception as exc:  # noqa: BLE001
            task.status = "failed"
            task.error = str(exc)
            raise
        finally:
            with self._lock:
                self.active_exports[export_id] = task
        return self.serialize_task(task)

    def get_status(self, export_id: str) -> Dict[str, Any]:
        task = self.active_exports.get(export_id)
        if not task:
            raise ExportTaskError("导出任务不存在")
        return self.serialize_task(task)

    def list_exports(self) -> List[Dict[str, Any]]:
        return [self.serialize_task(task) for task in self.active_exports.values()]

    def delete_export(self, export_id: str) -> bool:
        task = self.active_exports.get(export_id)
        if not task:
            return False
        if task.file_path and os.path.exists(task.file_path):
            os.remove(task.file_path)
        with self._lock:
            self.active_exports.pop(export_id, None)
        return True

    def cleanup_expired(self, retention_hours: int) -> int:
        deadline = datetime.now() - timedelta(hours=retention_hours)
        removed = 0
        for export_id, task in list(self.active_exports.items()):
            completed_at = None
            if task.completed_at:
                completed_at = datetime.fromisoformat(task.completed_at)
            if completed_at and completed_at < deadline:
                if self.delete_export(export_id):
                    removed += 1
        return removed

    def serialize_task(self, task: ExportTask) -> Dict[str, Any]:
        return {
            "export_id": task.export_id,
            "connection_id": task.connection_id,
            "export_type": task.export_type,
            "params": task.params,
            "status": task.status,
            "file_path": task.file_path,
            "file_size": task.file_size,
            "created_at": task.created_at,
            "completed_at": task.completed_at,
            "download_url": task.download_url,
            "error": task.error,
        }

    def _export_table(self, registry, task: ExportTask, file_path: str) -> None:
        table = task.params["table"]
        columns = task.params.get("columns", ["*"])
        where = task.params.get("where")
        select_columns = ", ".join(columns) if columns and columns != ["*"] else "*"
        sql = f"SELECT {select_columns} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        self._write_query_results(registry, task.connection_id, sql, file_path)

    def _export_query(self, registry, task: ExportTask, file_path: str) -> None:
        sql = task.params["sql"]
        self._write_query_results(registry, task.connection_id, sql, file_path)

    def _export_stream(self, registry, task: ExportTask, file_path: str) -> None:
        batch_size = int(task.params.get("batch_size", 1000))
        table = task.params["table"]
        sql = f"SELECT * FROM {table}"
        self._write_query_results(registry, task.connection_id, sql, file_path, batch_size=batch_size)

    def _write_query_results(self, registry, connection_id: str, sql: str, file_path: str, batch_size: int = 1000) -> None:
        connection = registry.get_connection(connection_id)
        cursor = connection.cursor
        cursor.execute(sql)
        columns = [description[0] for description in cursor.description]
        with open(file_path, "w", encoding="utf-8", newline="") as handler:
            writer = csv.writer(handler)
            writer.writerow(columns)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                writer.writerows([[normalize_value(item) for item in row] for row in rows])
        if not connection.auto_commit:
            connection.conn.commit()


__all__ = ["ExportManager", "ExportTask"]
