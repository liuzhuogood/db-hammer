"""Minimal Flask stub for offline testing."""
from __future__ import annotations

from typing import Any, Callable, Dict


class Request:
    def __init__(self) -> None:
        self._json: Dict[str, Any] = {}

    def set_json(self, data: Dict[str, Any]) -> None:
        self._json = data

    def get_json(self, force: bool = False) -> Dict[str, Any]:  # pragma: no cover - simple stub
        return self._json


request = Request()


class Blueprint:
    def __init__(self, name: str, import_name: str, url_prefix: str | None = None) -> None:
        self.name = name
        self.import_name = import_name
        self.url_prefix = url_prefix
        self.routes: Dict[str, Callable[..., Any]] = {}

    def route(self, rule: str, methods: list[str] | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.routes[rule] = func
            return func

        return decorator


class Flask:
    def __init__(self, import_name: str, template_folder: str | None = None, static_folder: str | None = None) -> None:
        self.import_name = import_name
        self.template_folder = template_folder
        self.static_folder = static_folder
        self.blueprints: Dict[str, Blueprint] = {}
        self.view_functions: Dict[str, Callable[..., Any]] = {}

    def register_blueprint(self, blueprint: Blueprint) -> None:
        self.blueprints[blueprint.name] = blueprint

    def route(self, rule: str, methods: list[str] | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.view_functions[rule] = func
            return func

        return decorator

    def run(self, host: str = "0.0.0.0", port: int = 8000) -> None:  # pragma: no cover - stub
        print(f"Running stub Flask app on {host}:{port}")

    def test_client(self):  # pragma: no cover - stub
        return self


def jsonify(data: Any) -> Any:
    return data


def render_template(template_name: str, **context: Any) -> Dict[str, Any]:
    return {"template": template_name, "context": context}
