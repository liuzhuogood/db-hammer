"""MCP相关异常定义"""


class MCPServerError(Exception):
    """基础异常"""


class AuthenticationError(MCPServerError):
    """认证失败"""


class AuthorizationError(MCPServerError):
    """权限异常"""


class ConnectionNotFoundError(MCPServerError):
    """连接不存在"""


class ExportTaskError(MCPServerError):
    """导出任务异常"""


class ValidationError(MCPServerError):
    """参数校验异常"""


__all__ = [
    "MCPServerError",
    "AuthenticationError",
    "AuthorizationError",
    "ConnectionNotFoundError",
    "ExportTaskError",
    "ValidationError",
]
