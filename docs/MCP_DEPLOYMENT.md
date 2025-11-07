# 部署指南

## 本地部署

```bash
pip install db-hammer[mcp]
cp db_hammer/mcp/templates/config.yaml ./mcp_config.yaml
db_hammer_mcp --config mcp_config.yaml --web
```

## Docker 示例

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install .[mcp]
CMD ["db_hammer_mcp", "--config", "/app/mcp_config.yaml", "--web"]
```

将配置文件挂载到容器内的 `/app/mcp_config.yaml` 即可。
