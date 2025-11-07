"""Data export tools for the MCP server."""
from __future__ import annotations

import csv
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:  # pragma: no cover
    from fastmcp import tool
except ImportError:  # pragma: no cover
    def tool(func=None, **kwargs):  # type: ignore
        if func is None:
            return lambda wrapped: wrapped
        return func

from ..exceptions import ExportError
from .connection import use_connection


class ExportTask(Dict[str, object]):
    pass


class ExportManager:
    """Manage export tasks and metadata."""

    def __init__(self, storage_path: str = "./exports"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self._tasks: Dict[str, ExportTask] = {}
        self._lock = threading.RLock()

    def create_export_task(self, connection_id: str, export_type: str, params: Dict[str, object]) -> str:
        export_id = str(uuid.uuid4())
        task: ExportTask = {
            "export_id": export_id,
            "connection_id": connection_id,
            "export_type": export_type,
            "params": params,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "file_path": None,
            "file_size": 0,
            "download_url": None,
            "error": None,
        }
        with self._lock:
            self._tasks[export_id] = task
        return export_id

    def execute_export(self, export_id: str) -> ExportTask:
        with self._lock:
            task = self._tasks.get(export_id)
        if not task:
            raise ExportError(f"导出任务不存在: {export_id}")
        task["status"] = "running"
        try:
            params = task["params"]
            file_extension = params.get("format", "csv")
            file_name = f"{export_id}.{file_extension}"
            file_path = self.storage_path / file_name
            if task["export_type"] == "table":
                self._export_table(task, file_path)
            elif task["export_type"] == "query":
                self._export_query(task, file_path)
            elif task["export_type"] == "stream":
                self._export_stream(task, file_path)
            else:
                raise ExportError(f"未知导出类型: {task['export_type']}")
            task["status"] = "completed"
            task["file_path"] = str(file_path)
            task["file_size"] = file_path.stat().st_size if file_path.exists() else 0
            task["download_url"] = f"/api/exports/{export_id}/download"
            task["completed_at"] = datetime.utcnow().isoformat()
        except Exception as exc:  # pragma: no cover - error path
            task["status"] = "failed"
            task["error"] = str(exc)
            raise
        finally:
            with self._lock:
                self._tasks[export_id] = task
        return task

    def get_task(self, export_id: str) -> Optional[ExportTask]:
        with self._lock:
            return self._tasks.get(export_id)

    def list_tasks(self) -> List[ExportTask]:
        with self._lock:
            return list(self._tasks.values())

    def delete_task(self, export_id: str) -> bool:
        with self._lock:
            task = self._tasks.pop(export_id, None)
        if not task:
            return False
        file_path = task.get("file_path")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        return True

    # Export implementations
    def _export_table(self, task: ExportTask, file_path: Path) -> None:
        params = task["params"]
        table = params.get("table")
        where = params.get("where")
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        self._export_query_rows(task, file_path, sql)

    def _export_query(self, task: ExportTask, file_path: Path) -> None:
        sql = task["params"].get("sql")
        self._export_query_rows(task, file_path, sql)

    def _export_stream(self, task: ExportTask, file_path: Path) -> None:
        params = task["params"]
        table = params.get("table")
        batch_size = int(params.get("batch_size", 1000))
        format_name = params.get("format", "csv")
        if format_name != "csv":
            raise ExportError("当前仅支持CSV格式的流式导出")
        where = params.get("where")
        with use_connection(task["connection_id"]) as connection:
            offset = 0
            headers_written = False
            with file_path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                while True:
                    sql = f"SELECT * FROM {table}"
                    if where:
                        sql += f" WHERE {where}"
                    sql += f" LIMIT {batch_size} OFFSET {offset}"
                    rows = connection.select_list(sql)
                    if not rows:
                        break
                    if not headers_written:
                        headers = [desc[0] for desc in connection.cursor.description]
                        writer.writerow(headers)
                        headers_written = True
                    writer.writerows(rows)
                    offset += batch_size

    def _export_query_rows(self, task: ExportTask, file_path: Path, sql: str) -> None:
        format_name = task["params"].get("format", "csv")
        if format_name != "csv":
            raise ExportError("当前仅支持导出为CSV")
        with use_connection(task["connection_id"]) as connection:
            rows = connection.select_list(sql)
            headers = [desc[0] for desc in connection.cursor.description]
            with file_path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(headers)
                writer.writerows(rows)


_MANAGER = ExportManager()


def _create_and_run(connection_id: str, export_type: str, params: Dict[str, object]) -> Dict[str, object]:
    export_id = _MANAGER.create_export_task(connection_id, export_type, params)
    task = _MANAGER.execute_export(export_id)
    return {"export_id": export_id, "status": task["status"], "download_url": task.get("download_url")}


@tool
def export_table_data(connection_id: str, table: str, format: str = "csv", where: Optional[str] = None) -> Dict[str, object]:
    params = {"table": table, "format": format, "where": where}
    return _create_and_run(connection_id, "table", params)


@tool
def export_query_data(connection_id: str, sql: str, format: str = "csv") -> Dict[str, object]:
    params = {"sql": sql, "format": format}
    return _create_and_run(connection_id, "query", params)


@tool
def stream_large_table(
    connection_id: str,
    table: str,
    batch_size: int = 1000,
    format: str = "csv",
    where: Optional[str] = None,
) -> Dict[str, object]:
    params = {"table": table, "batch_size": batch_size, "format": format, "where": where}
    return _create_and_run(connection_id, "stream", params)


@tool
def get_export_status(export_id: str) -> Dict[str, object]:
    task = _MANAGER.get_task(export_id)
    if not task:
        raise ExportError("导出任务不存在")
    return task


@tool
def download_export_file(export_id: str) -> str:
    task = _MANAGER.get_task(export_id)
    if not task or task.get("status") != "completed":
        raise ExportError("导出文件不可用")
    return task.get("file_path")


@tool
def list_export_files() -> List[Dict[str, object]]:
    return _MANAGER.list_tasks()


@tool
def delete_export_file(export_id: str) -> bool:
    return _MANAGER.delete_task(export_id)


__all__ = [
    "export_table_data",
    "export_query_data",
    "stream_large_table",
    "get_export_status",
    "download_export_file",
    "list_export_files",
    "delete_export_file",
    "ExportManager",
]
