"""Tools exposed through the MCP server."""
from .connection import (
    ConnectionManager,
    close_connection,
    create_mssql_connection,
    create_mysql_connection,
    create_oracle_connection,
    create_postgresql_connection,
    create_sqlite_connection,
    list_connections,
)
from .query import (
    execute_query,
    execute_select,
    execute_query_with_pagination,
    explain_query,
)
from .crud import insert_data, update_data, delete_data, upsert_data
from .export import (
    export_table_data,
    export_query_data,
    stream_large_table,
    get_export_status,
    download_export_file,
    list_export_files,
    delete_export_file,
)
from .schema import (
    describe_table,
    list_tables,
    get_table_schema,
    get_table_data,
)

__all__ = [
    "ConnectionManager",
    "create_mysql_connection",
    "create_postgresql_connection",
    "create_oracle_connection",
    "create_mssql_connection",
    "create_sqlite_connection",
    "close_connection",
    "list_connections",
    "execute_query",
    "execute_select",
    "execute_query_with_pagination",
    "explain_query",
    "insert_data",
    "update_data",
    "delete_data",
    "upsert_data",
    "export_table_data",
    "export_query_data",
    "stream_large_table",
    "get_export_status",
    "download_export_file",
    "list_export_files",
    "delete_export_file",
    "describe_table",
    "list_tables",
    "get_table_schema",
    "get_table_data",
]
