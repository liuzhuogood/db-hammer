# 部署指南

## 本地运行

```bash
pip install db-hammer[mcp]
db_hammer_mcp --config ./mcp_config.yaml --http
```

## Docker 镜像

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install .[mcp]
CMD ["db_hammer_mcp", "--config", "/app/mcp_config.yaml", "--http"]
```

## 生产建议

- 使用 `gunicorn` 或 `uwsgi` 暴露 Flask API
- 启用 HTTPS 代理以满足 `require_tls` 要求
- 配置环境变量 `DB_HAMMER_MCP_CONFIG` 指定配置文件路径
