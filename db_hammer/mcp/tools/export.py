"""Data export tools."""
from __future__ import annotations

import csv
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from ..fastmcp_adapter import FastMCP

from ..exceptions import ExportError
from .connection import registry


@dataclass
class ExportTask:
    export_id: str
    connection_id: str
    export_type: str
    params: Dict[str, object]
    status: str = "pending"
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    file_path: Optional[str] = None
    file_size: int = 0
    download_url: Optional[str] = None
    error: Optional[str] = None
    completed_at: Optional[str] = None


class ExportManager:
    def __init__(self, storage_path: str = "./exports") -> None:
        self.storage_path = storage_path
        os.makedirs(self.storage_path, exist_ok=True)
        self.active_exports: Dict[str, ExportTask] = {}

    def create_export_task(self, connection_id: str, export_type: str, params: Dict[str, object]) -> str:
        export_id = str(uuid.uuid4())
        task = ExportTask(export_id=export_id, connection_id=connection_id, export_type=export_type, params=params)
        self.active_exports[export_id] = task
        return export_id

    def _write_csv(self, file_path: str, headers: Iterable[str], rows: Iterable[Iterable]) -> None:
        with open(file_path, "w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)

    def _export_table(self, task: ExportTask, file_path: str) -> None:
        connection = registry.get_connection(task.connection_id)
        table = task.params["table"]
        where = task.params.get("where")
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        connection.execute(sql)
        rows = connection.cursor.fetchall()
        headers = [col[0] for col in connection.cursor.description]
        self._write_csv(file_path, headers, rows)

    def _export_query(self, task: ExportTask, file_path: str) -> None:
        connection = registry.get_connection(task.connection_id)
        sql = task.params["sql"]
        connection.execute(sql)
        rows = connection.cursor.fetchall()
        headers = [col[0] for col in connection.cursor.description]
        self._write_csv(file_path, headers, rows)

    def _export_stream(self, task: ExportTask, file_path: str) -> None:
        connection = registry.get_connection(task.connection_id)
        table = task.params["table"]
        batch_size = int(task.params.get("batch_size", 1000))
        format_type = task.params.get("format", "csv")
        if format_type != "csv":
            raise ExportError("Only CSV streaming is supported")
        where = task.params.get("where")
        columns = task.params.get("columns")
        column_sql = ", ".join(columns) if columns else "*"
        sql = f"SELECT {column_sql} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        offset = 0
        headers_written = False
        with open(file_path, "w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            while True:
                paged_sql = f"{sql} LIMIT {batch_size} OFFSET {offset}"
                connection.execute(paged_sql)
                batch_rows = connection.cursor.fetchall()
                if not batch_rows:
                    break
                if not headers_written:
                    headers = [col[0] for col in connection.cursor.description]
                    writer.writerow(headers)
                    headers_written = True
                for row in batch_rows:
                    writer.writerow(row)
                offset += batch_size

    def execute_export(self, export_id: str) -> Dict[str, object]:
        task = self.active_exports.get(export_id)
        if not task:
            raise ExportError(f"Export task not found: {export_id}")

        try:
            task.status = "running"
            file_name = f"{export_id}.{task.params.get('format', 'csv')}"
            file_path = os.path.join(self.storage_path, file_name)
            if task.export_type == "table":
                self._export_table(task, file_path)
            elif task.export_type == "query":
                self._export_query(task, file_path)
            elif task.export_type == "stream":
                self._export_stream(task, file_path)
            else:
                raise ExportError(f"Unknown export type: {task.export_type}")

            task.status = "completed"
            task.file_path = file_path
            task.file_size = os.path.getsize(file_path)
            task.download_url = f"/api/exports/download/{export_id}"
            task.completed_at = datetime.utcnow().isoformat()
        except Exception as exc:  # pragma: no cover - defensive
            task.status = "failed"
            task.error = str(exc)
            raise
        return task.__dict__

    def get_export(self, export_id: str) -> Dict[str, object]:
        task = self.active_exports.get(export_id)
        if not task:
            raise ExportError(f"Export task not found: {export_id}")
        return task.__dict__

    def list_exports(self) -> List[Dict[str, object]]:
        return [task.__dict__ for task in self.active_exports.values()]

    def delete_export(self, export_id: str) -> bool:
        task = self.active_exports.pop(export_id, None)
        if not task:
            return False
        if task.file_path and os.path.exists(task.file_path):
            os.remove(task.file_path)
        return True


def register_tools(app: FastMCP, manager: ExportManager) -> None:
    @app.tool()
    def export_table_data(connection_id: str, table: str, format: str = "csv", where: str | None = None) -> Dict[str, object]:
        export_id = manager.create_export_task(connection_id, "table", {"table": table, "format": format, "where": where})
        return manager.execute_export(export_id)

    @app.tool()
    def export_query_data(connection_id: str, sql: str, format: str = "csv") -> Dict[str, object]:
        export_id = manager.create_export_task(connection_id, "query", {"sql": sql, "format": format})
        return manager.execute_export(export_id)

    @app.tool()
    def stream_large_table(
        connection_id: str,
        table: str,
        batch_size: int = 1000,
        format: str = "csv",
    ) -> Dict[str, object]:
        export_id = manager.create_export_task(
            connection_id,
            "stream",
            {"table": table, "batch_size": batch_size, "format": format},
        )
        return manager.execute_export(export_id)

    @app.tool()
    def get_export_status(export_id: str) -> Dict[str, object]:
        return manager.get_export(export_id)

    @app.tool()
    def download_export_file(export_id: str) -> str:
        task = manager.get_export(export_id)
        file_path = task.get("file_path")
        if not file_path:
            raise ExportError("Export file not ready")
        return file_path

    @app.tool()
    def list_export_files() -> List[Dict[str, object]]:
        return manager.list_exports()

    @app.tool()
    def delete_export_file(export_id: str) -> bool:
        return manager.delete_export(export_id)
