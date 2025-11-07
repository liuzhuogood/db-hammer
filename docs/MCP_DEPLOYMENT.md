# MCP 部署指南

## 本地部署

```bash
pip install db-hammer[mcp]
cp db_hammer/mcp/templates/config.yaml mcp_config.yaml
db_hammer_mcp --config mcp_config.yaml --api-key <your-key>
```

可通过 `--host`、`--port` 指定监听地址，也可以在配置文件中添加多条连接。

## Docker 部署

创建 `Dockerfile`：

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install .[mcp]
EXPOSE 8000
CMD ["db_hammer_mcp", "--config", "/app/mcp_config.yaml", "--host", "0.0.0.0"]
```

构建并运行：

```bash
docker build -t db-hammer-mcp .
docker run -p 8000:8000 -v $(pwd)/mcp_config.yaml:/app/mcp_config.yaml db-hammer-mcp
```

## 云主机部署

1. 准备一台 Linux 云服务器，安装 Python 3.10+
2. `git clone` 或 `pip install db-hammer[mcp]`
3. 上传配置文件并运行 `db_hammer_mcp`
4. 建议在前面添加反向代理（Nginx），开启 HTTPS 与访问控制

## 生产建议

- 使用系统服务（systemd、supervisor）守护进程，确保崩溃后自动重启。
- 定期清理导出目录，可开启配置中的 `auto_cleanup`。
- 配置防火墙或安全组，仅允许可信来源访问 MCP 端口。
