"""Custom exceptions for the MCP server."""


class MCPError(Exception):
    """Base exception for MCP server errors."""


class ConfigurationError(MCPError):
    """Raised when configuration fails to load or validate."""


class AuthenticationError(MCPError):
    """Raised when authentication fails."""


class AuthorizationError(MCPError):
    """Raised when authorization is denied."""


class ConnectionNotFoundError(MCPError):
    """Raised when a connection id is not registered."""


class ExportError(MCPError):
    """Raised when an export task fails."""
