"""Custom exceptions used by the db-hammer MCP server."""

from __future__ import annotations


class MCPError(Exception):
    """Base class for MCP related errors."""


class ConfigurationError(MCPError):
    """Raised when configuration files are invalid or missing entries."""


class AuthenticationError(MCPError):
    """Raised when authentication fails."""


class AuthorizationError(MCPError):
    """Raised when a caller is not allowed to perform an action."""


class ConnectionError(MCPError):
    """Raised when creating or using a database connection fails."""


class QueryError(MCPError):
    """Raised when an SQL query cannot be executed."""


class ExportError(MCPError):
    """Raised when exporting query results fails."""
