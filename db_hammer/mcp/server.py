"""MCP服务器实现"""
from __future__ import annotations

import contextvars
import logging
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, Optional

from .auth import AuthManager
from .config import ConfigManager
from .exceptions import AuthenticationError, MCPServerError
from .export import ExportManager
from .web.simple_app import SimpleApp, SimpleRequest, SimpleResponse, TestClient, jsonify
from .web.app import create_web_blueprint

LOGGER = logging.getLogger(__name__)

TOOL_REGISTRY: Dict[str, "ToolDefinition"] = {}
RESOURCE_REGISTRY: Dict[str, "ResourceDefinition"] = {}
CURRENT_CONTEXT: contextvars.ContextVar["ToolContext"] = contextvars.ContextVar("db_hammer_mcp_context")


@dataclass
class ToolContext:
    config_manager: ConfigManager
    export_manager: ExportManager
    auth_manager: AuthManager
    registry: "ConnectionRegistry"


@dataclass
class ToolDefinition:
    name: str
    description: Optional[str]
    func: Callable[..., Any]


@dataclass
class ResourceDefinition:
    name: str
    description: Optional[str]
    handler: Callable[[Dict[str, Any]], Any]


def get_tool_context() -> ToolContext:
    return CURRENT_CONTEXT.get()


def mcp_tool(func: Optional[Callable[..., Any]] = None, *, name: Optional[str] = None, description: Optional[str] = None):
    def decorator(target: Callable[..., Any]) -> Callable[..., Any]:
        tool_name = name or target.__name__
        TOOL_REGISTRY[tool_name] = ToolDefinition(name=tool_name, description=description, func=target)
        return target

    if func is not None:
        return decorator(func)
    return decorator


def register_resource(name: str, description: Optional[str] = None):
    def wrapper(handler: Callable[[Dict[str, Any]], Any]) -> Callable[[Dict[str, Any]], Any]:
        RESOURCE_REGISTRY[name] = ResourceDefinition(name=name, description=description, handler=handler)
        return handler

    return wrapper


class DBHammerMCPServer:
    """轻量级MCP服务实现"""

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_manager = ConfigManager(config_path)
        from .tools.connection import ConnectionRegistry  # 延迟导入避免循环

        self.registry = ConnectionRegistry(self.config_manager)
        self.export_manager = ExportManager(self.config_manager.get_exports_path())
        self.auth_manager = AuthManager(self.config_manager)
        self.app = SimpleApp()
        self.context = ToolContext(
            config_manager=self.config_manager,
            export_manager=self.export_manager,
            auth_manager=self.auth_manager,
            registry=self.registry,
        )
        self._register_routes()

    def _register_routes(self) -> None:
        self.app.register_blueprint(create_web_blueprint(self.config_manager, self.registry, self.export_manager, self.auth_manager))

        @self.app.route("/api/tools", methods=["GET"])
        def list_tools(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
            self._authenticate(request.headers)
            payload = {name: {"description": tool.description} for name, tool in TOOL_REGISTRY.items()}
            return jsonify(payload)

        @self.app.route("/api/tools/<tool_name>", methods=["POST"])
        def execute_tool(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:
            identity = self._authenticate(request.headers)
            tool_name = params["tool_name"]
            LOGGER.info("执行工具 %s by %s", tool_name, identity)
            result = self._invoke_tool(tool_name, request.json())
            return jsonify({"status": "success", "data": result, "tool": tool_name})

        @self.app.route("/api/resources", methods=["GET"])
        def list_resources(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
            self._authenticate(request.headers)
            payload = {name: {"description": resource.description} for name, resource in RESOURCE_REGISTRY.items()}
            return jsonify(payload)

        @self.app.route("/api/resources/<resource_name>", methods=["GET"])
        def fetch_resource(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:
            identity = self._authenticate(request.headers)
            resource_name = params["resource_name"]
            LOGGER.debug("资源访问 %s by %s", resource_name, identity)
            resource = RESOURCE_REGISTRY.get(resource_name)
            if not resource:
                return jsonify({"error": "resource_not_found"}, status=404)
            token = CURRENT_CONTEXT.set(self.context)
            try:
                data = resource.handler(request.query_params)
            finally:
                CURRENT_CONTEXT.reset(token)
            return jsonify({"resource": resource_name, "data": data})

        @self.app.route("/api/exports", methods=["GET"])
        def list_exports(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:  # noqa: ARG001
            self._authenticate(request.headers)
            return jsonify(self.export_manager.list_exports())

        @self.app.route("/api/exports/<export_id>", methods=["GET"])
        def export_status(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:
            self._authenticate(request.headers)
            status = self.export_manager.get_status(params["export_id"])
            return jsonify(status)

        @self.app.route("/api/exports/<export_id>", methods=["DELETE"])
        def delete_export(request: SimpleRequest, params: Dict[str, str]) -> SimpleResponse:
            self._authenticate(request.headers)
            deleted = self.export_manager.delete_export(params["export_id"])
            return jsonify({"deleted": deleted})

    def _authenticate(self, headers: Dict[str, str]) -> str:
        return self.auth_manager.authenticate_headers(headers)

    def _invoke_tool(self, tool_name: str, payload: Dict[str, Any]) -> Any:
        tool = TOOL_REGISTRY.get(tool_name)
        if not tool:
            raise MCPServerError(f"工具未注册: {tool_name}")
        token = CURRENT_CONTEXT.set(self.context)
        try:
            return tool.func(**payload)
        finally:
            CURRENT_CONTEXT.reset(token)

    def run(self, host: str = "0.0.0.0", port: int = 8000, debug: bool = False) -> None:  # noqa: ARG002
        server = self

        class RequestHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                self._handle("GET")

            def do_POST(self):  # noqa: N802
                self._handle("POST")

            def do_DELETE(self):  # noqa: N802
                self._handle("DELETE")

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                LOGGER.debug(format, *args)

            def _handle(self, method: str) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b""
                headers = {k: v for k, v in self.headers.items()}
                try:
                    response = server.app.dispatch(method, self.path, headers, body)
                except AuthenticationError as exc:  # type: ignore[misc]
                    LOGGER.warning("认证失败: %s", exc)
                    response = jsonify({"error": "unauthorized", "message": str(exc)}, status=401)
                except MCPServerError as exc:  # type: ignore[misc]
                    LOGGER.error("服务器异常: %s", exc)
                    response = jsonify({"error": "mcp_error", "message": str(exc)}, status=400)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("未捕获异常: %s", exc)
                    response = jsonify({"error": "internal_error", "message": "服务器内部错误"}, status=500)
                status, headers_list, payload = response.to_wsgi()
                self.send_response(status)
                for key, value in headers_list:
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(payload)

        httpd = HTTPServer((host, port), RequestHandler)
        LOGGER.info("MCP Server 启动在 %s:%s", host, port)
        httpd.serve_forever()

    def test_client(self) -> TestClient:
        return self.app.test_client()


__all__ = [
    "DBHammerMCPServer",
    "ToolContext",
    "mcp_tool",
    "register_resource",
    "get_tool_context",
]
