"""Command line entry point for the db-hammer MCP server."""
from __future__ import annotations

import argparse
import logging
import threading
from typing import Optional

from ..mcp.config import load_config
from ..mcp.server import create_server
from ..mcp.web.app import create_app

LOGGER = logging.getLogger(__name__)


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动 db-hammer MCP Server")
    parser.add_argument("--config", help="配置文件路径", default=None)
    parser.add_argument("--host", help="服务监听地址", default=None)
    parser.add_argument("--port", help="服务端口", type=int, default=None)
    parser.add_argument("--web", help="启动Web配置界面", action="store_true")
    parser.add_argument("--web-port", help="Web界面端口", type=int, default=8900)
    return parser.parse_args(argv)


def run(argv: Optional[list] = None) -> None:  # pragma: no cover - CLI entry point
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    config = load_config(args.config)
    if args.host:
        config.server.host = args.host
    if args.port:
        config.server.port = args.port

    server = create_server(config)

    if args.web and config.server.web_enabled:
        app = create_app(args.config)

        def run_web():
            LOGGER.info("Web配置界面启动: http://%s:%s", config.server.host, args.web_port)
            app.run(host=config.server.host, port=args.web_port, debug=config.server.debug)

        threading.Thread(target=run_web, daemon=True).start()

    LOGGER.info("MCP服务启动: %s:%s", config.server.host, config.server.port)
    server.serve_forever()


def main() -> None:  # pragma: no cover
    run()


if __name__ == "__main__":  # pragma: no cover
    main()
