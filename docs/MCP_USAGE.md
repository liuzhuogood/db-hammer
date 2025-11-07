# db-hammer MCP 快速开始

本文介绍如何在5分钟内运行 db-hammer MCP 服务并通过HTTP调用数据库工具。

## 1. 安装

```bash
pip install db-hammer[mcp]
```

## 2. 准备配置

使用仓库提供的模板：

```bash
cp db_hammer/mcp/templates/config.yaml mcp_config.yaml
```

修改 `mcp_config.yaml`，确保 `connections` 中包含可用的数据库连接，至少保留示例的 SQLite 连接。

## 3. 启动服务

```bash
db_hammer_mcp --config mcp_config.yaml --api-key demo-key
```

默认监听 `0.0.0.0:8000`，可通过 `--host`、`--port` 调整。

## 4. 访问工具

### 4.1 列出工具

```bash
curl -H "X-API-Key: demo-key" http://127.0.0.1:8000/api/tools
```

### 4.2 创建SQLite连接

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -H "X-API-Key: demo-key" \
     -d '{"database_path": "./demo.db"}' \
     http://127.0.0.1:8000/api/tools/create_sqlite_connection
```

返回示例：

```json
{"status":"success","tool":"create_sqlite_connection","data":{"connection_id":"..."}}
```

### 4.3 执行查询

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -H "X-API-Key: demo-key" \
     -d '{"connection_id": "<上一步的ID>", "sql": "SELECT 1"}' \
     http://127.0.0.1:8000/api/tools/execute_query
```

## 5. Web 控制台

浏览器访问 `http://127.0.0.1:8000/`，首次访问会提示输入 API Key，可在线查看和更新配置、测试连接。

---

更多示例请查看 `docs/MCP_API.md`。
