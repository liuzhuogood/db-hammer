"""Command line entry point for db-hammer MCP server."""
from __future__ import annotations

import argparse
import logging
import sys

from db_hammer.mcp.server import run


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the db-hammer MCP server")
    parser.add_argument("--config", help="Path to MCP configuration file", default=None)
    parser.add_argument("--log-level", help="Logging level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    run(args.config)


if __name__ == "__main__":
    main(sys.argv[1:])
