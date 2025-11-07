# db-hammer MCP 使用指南

本文档介绍如何在本地快速启动 db-hammer MCP Server 并通过 HTTP 或 MCP 协议进行访问。

## 快速开始

```bash
pip install db-hammer[mcp]
db_hammer_mcp --config path/to/mcp_config.yaml --http
```

服务器启动后，可通过浏览器访问 `http://127.0.0.1:8000/` 查看商务简洁风格的配置页面。

## 主要能力

- 多数据库连接管理（MySQL、PostgreSQL、Oracle、MSSQL、SQLite）
- SQL 查询与分页执行
- 数据增删改查
- 表结构与索引浏览
- 数据导出 CSV
- API Key / JWT 双模式认证

## 关键 API

| Endpoint | 方法 | 说明 |
| --- | --- | --- |
| `/api/config` | GET/POST | 读取与保存配置 |
| `/api/connections` | GET | 查看当前连接 |
| `/api/connections/test` | POST | 测试数据库连接 |
| `/api/exports` | GET/POST | 导出任务管理 |

更多工具说明请参考 `docs/MCP_API.md`。
