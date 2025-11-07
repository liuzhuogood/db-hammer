# MCP 部署指南

## 本地

```bash
pip install db-hammer[mcp]
db_hammer_mcp --config mcp_config.yaml
```

## Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install .[mcp]
CMD ["db_hammer_mcp", "--config", "/app/mcp_config.yaml"]
```
