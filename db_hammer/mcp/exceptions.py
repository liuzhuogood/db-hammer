"""Custom exceptions for the db-hammer MCP server."""
from __future__ import annotations


class MCPError(RuntimeError):
    """Base error for MCP server issues."""


class ConfigurationError(MCPError):
    """Raised when configuration related operations fail."""


class ConnectionError(MCPError):
    """Raised when database connections fail."""


class AuthorizationError(MCPError):
    """Raised when authentication or authorization fails."""


class ExportError(MCPError):
    """Raised when data export operations fail."""
