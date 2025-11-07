# MCP API 文档

以下列出主要工具和请求示例。所有请求均需携带 `X-API-Key` 或 `Authorization: Bearer <token>`。

## 工具分类

### 1. 连接管理
- `create_mysql_connection`
- `create_postgresql_connection`
- `create_oracle_connection`
- `create_mssql_connection`
- `create_sqlite_connection`
- `list_connections`
- `close_connection`

### 2. 查询执行
- `execute_query`
- `execute_select`
- `execute_query_with_pagination`
- `explain_query`

### 3. 数据操作
- `insert_data`
- `update_data`
- `delete_data`
- `upsert_data`

### 4. 数据导出
- `export_table_data`
- `export_query_data`
- `stream_large_table`
- `get_export_status`
- `download_export_file`
- `list_export_files`
- `delete_export_file`

### 5. 管理工具
- `ping_database`
- `get_database_version`
- `get_server_time`
- `run_maintenance`

### 6. 结构资源
- `list_tables`
- `describe_table`
- `list_schemas`

## REST 调用格式

```
POST /api/tools/<tool_name>
Content-Type: application/json
X-API-Key: <your key>

{"param": "value"}
```

返回：

```json
{"status": "success", "tool": "tool_name", "data": {...}}
```

## 资源接口

- `GET /api/resources` 查看资源列表
- `GET /api/resources/tables?connection_id=<id>` 列出表
- `GET /api/resources/table_columns?connection_id=<id>&table=<name>` 表字段
- `GET /api/resources/schemas?connection_id=<id>` 列出Schema

## 导出接口

- `GET /api/exports` 查看导出任务
- `GET /api/exports/<export_id>` 状态
- `GET /api/exports/download/<export_id>` 下载
- `DELETE /api/exports/<export_id>` 删除文件

## 认证接口

- `POST /api/auth/token` 使用 API Key 换取临时 Token：

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"api_key": "demo-key"}' \
     http://127.0.0.1:8000/api/auth/token
```

返回：

```json
{"token": "<jwt>"}
```

---

更多使用示例请参考 `tests/` 中的单元测试。
