"""导出工具"""
from __future__ import annotations

from typing import Dict, Optional

from ..exceptions import ExportTaskError
from ..server import get_tool_context, mcp_tool
from ..utils.validator import validate_columns, validate_table_name


def _run_export(export_type: str, params: Dict[str, object]) -> Dict[str, object]:
    context = get_tool_context()
    export_id = context.export_manager.create_export_task(params["connection_id"], export_type, params)
    status = context.export_manager.execute_export(export_id, context.registry)
    return status


@mcp_tool(description="导出整张表")
def export_table_data(
    connection_id: str,
    table: str,
    format: str = "csv",
    where: Optional[str] = None,
    columns: Optional[list] = None,
) -> Dict[str, object]:
    validate_table_name(table)
    if columns:
        validate_columns(columns)
    params: Dict[str, object] = {
        "connection_id": connection_id,
        "table": table,
        "format": format,
    }
    if where:
        params["where"] = where
    if columns:
        params["columns"] = columns
    return _run_export("table", params)


@mcp_tool(description="导出自定义查询")
def export_query_data(connection_id: str, sql: str, format: str = "csv") -> Dict[str, object]:
    params = {"connection_id": connection_id, "sql": sql, "format": format}
    return _run_export("query", params)


@mcp_tool(description="流式导出大表")
def stream_large_table(
    connection_id: str,
    table: str,
    batch_size: int = 1000,
    format: str = "csv",
) -> Dict[str, object]:
    validate_table_name(table)
    params = {
        "connection_id": connection_id,
        "table": table,
        "batch_size": batch_size,
        "format": format,
    }
    return _run_export("stream", params)


@mcp_tool(description="导出状态查询")
def get_export_status(export_id: str) -> Dict[str, object]:
    context = get_tool_context()
    return context.export_manager.get_status(export_id)


@mcp_tool(description="下载导出文件")
def download_export_file(export_id: str) -> str:
    context = get_tool_context()
    task = context.export_manager.get_status(export_id)
    file_path = task.get("file_path")
    if not file_path:
        raise ExportTaskError("导出文件不存在")
    return str(file_path)


@mcp_tool(description="列出导出文件")
def list_export_files() -> Dict[str, object]:
    context = get_tool_context()
    return {"items": context.export_manager.list_exports()}


@mcp_tool(description="删除导出文件")
def delete_export_file(export_id: str) -> bool:
    context = get_tool_context()
    return context.export_manager.delete_export(export_id)


__all__ = [
    "export_table_data",
    "export_query_data",
    "stream_large_table",
    "get_export_status",
    "download_export_file",
    "list_export_files",
    "delete_export_file",
]
