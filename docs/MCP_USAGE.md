# db-hammer MCP Server 快速开始

本文介绍如何使用 db-hammer 提供的 MCP Server 功能。

## 安装

```bash
pip install db-hammer[mcp]
```

## 启动服务

```bash
db_hammer_mcp --config mcp_config.yaml --web
```

启动后可以通过 `http://localhost:8900` 访问配置界面，MCP 服务默认监听 `8800` 端口。

## 使用步骤

1. 在配置界面中编辑数据库连接（配置文件使用 JSON 语法保存为 `.yaml` 文件）。
2. 点击“测试连接”确保配置正确。
3. 保存配置后，AI 助手即可通过 MCP 协议调用数据库工具。
