# MCP API 文档

## 工具 (Tools)

- `create_sqlite_connection(database_path: str)`
- `list_connections()`
- `execute_query(connection_id: str, sql: str, params: dict | None = None)`
- `execute_query_with_pagination(connection_id: str, sql: str, page_size: int = 100, page: int = 1)`
- `insert_data(connection_id: str, table: str, data: dict)`
- `update_data(connection_id: str, table: str, data: dict, where: str)`
- `delete_data(connection_id: str, table: str, where: str)`
- `upsert_data(connection_id: str, table: str, data: dict, conflict_columns: list)`
- `export_table_data(connection_id: str, table: str, format: str = "csv", where: str | None = None)`
- `export_query_data(connection_id: str, sql: str, format: str = "csv")`
- `stream_large_table(connection_id: str, table: str, batch_size: int = 1000, format: str = "csv")`

## 资源 (Resources)

- `databases://connections`
- `databases://{connection_id}/status`
- `databases://{connection_id}/tables`
- `databases://{connection_id}/tables/{table}/schema`
- `databases://{connection_id}/tables/{table}/data`
