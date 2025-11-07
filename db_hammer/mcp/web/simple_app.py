"""简易Web框架"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


class SimpleRequest:
    def __init__(self, method: str, path: str, headers: Dict[str, str], body: bytes) -> None:
        parsed = urlparse(path)
        self.method = method.upper()
        self.path = parsed.path
        self.query_params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}
        self.headers = {k: v for k, v in headers.items()}
        self._body = body
        self._json_cache: Optional[Dict[str, Any]] = None

    def json(self) -> Dict[str, Any]:
        if self._json_cache is None:
            if not self._body:
                self._json_cache = {}
            else:
                self._json_cache = json.loads(self._body.decode("utf-8"))
        return self._json_cache


@dataclass
class SimpleResponse:
    status: int
    body: Any
    headers: Dict[str, str]

    def to_wsgi(self) -> Tuple[int, List[Tuple[str, str]], bytes]:
        if isinstance(self.body, (dict, list)):
            payload = json.dumps(self.body).encode("utf-8")
            headers = {**{"Content-Type": "application/json"}, **self.headers}
        elif isinstance(self.body, str):
            payload = self.body.encode("utf-8")
            headers = {**{"Content-Type": "text/plain; charset=utf-8"}, **self.headers}
        else:
            payload = self.body if isinstance(self.body, bytes) else str(self.body).encode("utf-8")
            headers = self.headers
        return self.status, list(headers.items()), payload


@dataclass
class Route:
    methods: Iterable[str]
    pattern: re.Pattern
    handler: Callable[[SimpleRequest, Dict[str, str]], SimpleResponse]


class Blueprint:
    def __init__(self, name: str, url_prefix: str = "") -> None:
        self.name = name
        self.url_prefix = url_prefix.rstrip("/")
        self._routes: List[Tuple[str, str, Callable[[SimpleRequest, Dict[str, str]], SimpleResponse]]] = []

    def route(self, rule: str, methods: Iterable[str]):
        def decorator(func: Callable[[SimpleRequest, Dict[str, str]], SimpleResponse]):
            self._routes.append((rule, [method.upper() for method in methods], func))
            return func

        return decorator

    def iter_routes(self) -> Iterable[Tuple[str, Iterable[str], Callable[[SimpleRequest, Dict[str, str]], SimpleResponse]]]:
        for rule, methods, handler in self._routes:
            yield f"{self.url_prefix}{rule}", methods, handler

    def register_blueprint(self, blueprint: "Blueprint") -> None:
        for rule, methods, handler in blueprint.iter_routes():
            self._routes.append((rule, list(methods), handler))


class SimpleApp:
    def __init__(self) -> None:
        self._routes: List[Route] = []

    def route(self, rule: str, methods: Iterable[str]):
        def decorator(func: Callable[[SimpleRequest, Dict[str, str]], SimpleResponse]):
            self._add_route(rule, methods, func)
            return func

        return decorator

    def register_blueprint(self, blueprint: Blueprint) -> None:
        for rule, methods, handler in blueprint.iter_routes():
            self._add_route(rule, methods, handler)

    def _add_route(self, rule: str, methods: Iterable[str], handler: Callable[[SimpleRequest, Dict[str, str]], SimpleResponse]):
        pattern = self._compile_rule(rule)
        self._routes.append(Route([method.upper() for method in methods], pattern, handler))

    def _compile_rule(self, rule: str) -> re.Pattern:
        regex = re.sub(r"<([a-zA-Z_][a-zA-Z0-9_]*)>", r"(?P<\1>[^/]+)", rule)
        return re.compile(f"^{regex}$")

    def dispatch(self, method: str, path: str, headers: Optional[Dict[str, str]] = None, body: bytes = b"") -> SimpleResponse:
        request = SimpleRequest(method, path, headers or {}, body)
        for route in self._routes:
            if method.upper() not in route.methods:
                continue
            match = route.pattern.match(request.path)
            if match:
                return route.handler(request, match.groupdict())
        return SimpleResponse(HTTPStatus.NOT_FOUND, {"error": "not_found"}, {})

    def test_client(self) -> "TestClient":
        return TestClient(self)


class TestClient:
    def __init__(self, app: SimpleApp) -> None:
        self.app = app

    def _call(self, method: str, path: str, headers: Optional[Dict[str, str]] = None, data: Optional[str] = None) -> "TestResponse":
        body = data.encode("utf-8") if data else b""
        response = self.app.dispatch(method, path, headers, body)
        status, headers_list, payload = response.to_wsgi()
        headers_dict = dict(headers_list)
        return TestResponse(status, payload, headers_dict)

    def get(self, path: str, headers: Optional[Dict[str, str]] = None) -> "TestResponse":
        return self._call("GET", path, headers=headers)

    def post(self, path: str, headers: Optional[Dict[str, str]] = None, data: Optional[str] = None) -> "TestResponse":
        return self._call("POST", path, headers=headers, data=data)

    def delete(self, path: str, headers: Optional[Dict[str, str]] = None) -> "TestResponse":
        return self._call("DELETE", path, headers=headers)


class TestResponse:
    def __init__(self, status_code: int, data: bytes, headers: Dict[str, str]) -> None:
        self.status_code = status_code
        self.data = data
        self.headers = headers

    def get_json(self) -> Any:
        if not self.data:
            return None
        return json.loads(self.data.decode("utf-8"))


def jsonify(payload: Any, status: int = 200) -> SimpleResponse:
    return SimpleResponse(status=status, body=payload, headers={})


__all__ = [
    "SimpleApp",
    "Blueprint",
    "SimpleRequest",
    "SimpleResponse",
    "TestClient",
    "jsonify",
]
