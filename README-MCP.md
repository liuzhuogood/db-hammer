# db-hammer MCP 功能概览

db-hammer MCP Server 为数据库操作提供统一的 MCP 接口，支持 MySQL、PostgreSQL、Oracle、MSSQL、SQLite 等主流数据库类型，并包含以下能力：

- 连接管理：创建、关闭、列出连接
- 查询执行：SQL 执行、分页、Explain
- 数据操作：增删改、Upsert
- 数据导出：表导出、SQL 导出、流式导出
- 元数据访问：表、列、Schema 资源
- 管理工具：版本查询、健康检查、维护操作
- Web 控制台：在线查看/编辑配置、测试连接

## 快速启动

```bash
pip install db-hammer[mcp]
db_hammer_mcp --config mcp_config.yaml --api-key demo-key
```

## 资源

- `docs/MCP_USAGE.md` 快速开始
- `docs/MCP_API.md` API 说明
- `docs/WEB_CONFIG.md` Web 控制台
- `docs/MCP_DEPLOYMENT.md` 部署指南

## 测试

本仓库提供 pytest 测试，确保核心功能覆盖：

```bash
pip install -r requirements.txt
pip install pytest
pytest
```

欢迎提交 Issue 或 PR 反馈改进建议。
