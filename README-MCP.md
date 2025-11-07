# db-hammer MCP 概述

该模块基于 fastmcp 协议实现数据库操作能力，支持通过 CLI、HTTP 页面以及 MCP 工具链访问。

## 功能

- 多数据库连接管理
- SQL 查询、分页、解释
- 数据增删改查与 UPSERT
- CSV 导出与下载
- 认证与审计钩子
- Web 控制台

## 启动

```bash
pip install db-hammer[mcp]
db_hammer_mcp --config ./mcp_config.yaml --http
```

若已安装 `fastmcp`，可以省略 `--http`，通过标准输入输出启动 MCP 服务。
