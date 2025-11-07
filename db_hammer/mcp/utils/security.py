"""安全工具函数"""
from __future__ import annotations

import os
import secrets
from hashlib import sha256
from typing import Dict

SENSITIVE_KEYS = {"password", "pwd", "secret", "token", "key"}


def secure_compare(first: str, second: str) -> bool:
    return secrets.compare_digest(first, second)


def mask_connection_params(params: Dict[str, str]) -> Dict[str, str]:
    masked = {}
    for key, value in params.items():
        if key.lower() in SENSITIVE_KEYS and value:
            hashed = sha256(value.encode("utf-8")).hexdigest()
            masked[key] = f"***{hashed[:6]}"
        else:
            masked[key] = value
    return masked


def generate_api_key(length: int = 40) -> str:
    return secrets.token_urlsafe(length)


def generate_secret(length: int = 32) -> str:
    return secrets.token_hex(length)


def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)


__all__ = [
    "secure_compare",
    "mask_connection_params",
    "generate_api_key",
    "generate_secret",
    "ensure_directory",
]
