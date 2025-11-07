"""轻量级YAML工具，优先使用PyYAML，缺失时回退到简易解析"""
from __future__ import annotations

import json
from typing import Any, Dict


def _fallback_load(text: str) -> Dict[str, Any]:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lines.append(line)
    normalized = "\n".join(lines)
    try:
        return json.loads(normalized)
    except json.JSONDecodeError as exc:
        raise ValueError("无法解析配置文本，请安装 PyYAML 或提供 JSON 格式配置") from exc


def safe_load(text: str) -> Dict[str, Any]:
    try:  # noqa: SIM105
        import yaml  # type: ignore

        return yaml.safe_load(text) or {}
    except ModuleNotFoundError:
        return _fallback_load(text)


def safe_dump(data: Dict[str, Any]) -> str:
    try:  # noqa: SIM105
        import yaml  # type: ignore

        return yaml.safe_dump(data, allow_unicode=True, sort_keys=True)
    except ModuleNotFoundError:
        return json.dumps(data, ensure_ascii=False, indent=2)


__all__ = ["safe_load", "safe_dump"]
