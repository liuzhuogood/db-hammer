"""Security helpers."""

from __future__ import annotations

import secrets
from hashlib import sha256


def hash_token(token: str) -> str:
    return sha256(token.encode("utf-8")).hexdigest()


def generate_api_key() -> str:
    return secrets.token_urlsafe(32)
