"""MCP Server 启动脚本"""
from __future__ import annotations

import argparse
import logging

from db_hammer.mcp import DBHammerMCPServer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动db-hammer MCP服务")
    parser.add_argument("--config", help="配置文件路径", default=None)
    parser.add_argument("--host", help="监听地址", default="0.0.0.0")
    parser.add_argument("--port", help="监听端口", default=8000, type=int)
    parser.add_argument("--debug", action="store_true", help="开启调试模式")
    parser.add_argument("--api-key", dest="api_keys", action="append", help="预置API Key，可多次传入")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    server = DBHammerMCPServer(config_path=args.config)
    if args.api_keys:
        for key in args.api_keys:
            server.auth_manager.register_api_key(key)
    logging.info("MCP Server 启动在 %s:%s", args.host, args.port)
    server.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":  # pragma: no cover
    main()
