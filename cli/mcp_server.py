"""Command line entry point for the db-hammer MCP server."""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from db_hammer.mcp import DBHammerMCPServer


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the db-hammer MCP server")
    parser.add_argument("--config", help="Path to the MCP configuration file", default=None)
    parser.add_argument("--host", help="HTTP host", default="127.0.0.1")
    parser.add_argument("--port", help="HTTP port", type=int, default=8000)
    parser.add_argument("--http", help="Force HTTP mode even if fastmcp is installed", action="store_true")
    parser.add_argument("--list-tools", action="store_true", help="List available tools and exit")
    parser.add_argument("--list-resources", action="store_true", help="List available resources and exit")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    server = DBHammerMCPServer(config_path=args.config)

    if args.list_tools:
        for name, description in server.list_tools().items():
            print(f"{name}: {description}")
        return

    if args.list_resources:
        for name, description in server.list_resources().items():
            print(f"{name}: {description}")
        return

    server.run(host=args.host, port=args.port, use_http=args.http)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
