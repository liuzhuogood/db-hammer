"""认证与授权模块"""
from __future__ import annotations

import base64
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Dict, Optional

from .config import ConfigManager
from .exceptions import AuthenticationError

LOGGER = logging.getLogger(__name__)


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _base64url_decode(data: str) -> bytes:
    padding = "=" * ((4 - len(data) % 4) % 4)
    return base64.urlsafe_b64decode(data + padding)


@dataclass
class AuthManager:
    """简单的API Key + Token认证实现"""

    config: ConfigManager
    audience: str = "db-hammer-mcp"
    _cached_hashes: Dict[str, str] = field(default_factory=dict)

    def register_api_key(self, api_key: str) -> None:
        LOGGER.debug("register api key")
        if api_key not in self.config.list_api_keys():
            self.config.add_api_key(api_key)
        self._cached_hashes.clear()

    def revoke_api_key(self, api_key: str) -> None:
        LOGGER.debug("revoke api key")
        self.config.remove_api_key(api_key)
        self._cached_hashes.clear()

    def authenticate_headers(self, headers: Dict[str, str]) -> Optional[str]:
        """根据请求头进行认证，返回认证主体"""

        api_key = headers.get("X-API-Key") or headers.get("x-api-key")
        if api_key and self._validate_api_key(api_key):
            return api_key

        authorization = headers.get("Authorization")
        if authorization and authorization.startswith("Bearer "):
            token = authorization.split(" ", 1)[1].strip()
            payload = self.verify_token(token)
            return payload.get("sub")

        if self.config._config.auth.allow_anonymous:
            return "anonymous"

        raise AuthenticationError("未提供有效的认证信息")

    def _validate_api_key(self, api_key: str) -> bool:
        hashed = self._hash(api_key)
        if not self._cached_hashes:
            self._cached_hashes = {self._hash(item): item for item in self.config.list_api_keys()}
        for stored_hash in self._cached_hashes:
            if hmac.compare_digest(stored_hash, hashed):
                return True
        return False

    def issue_token(self, subject: str, expires_in: Optional[int] = None) -> str:
        expires = int(time.time()) + (expires_in or self.config._config.auth.token_expire_seconds)
        payload = {"sub": subject, "aud": self.audience, "exp": expires}
        header = {"alg": "HS256", "typ": "JWT"}
        encoded_header = _base64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        encoded_payload = _base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        signature = self._sign(f"{encoded_header}.{encoded_payload}".encode("utf-8"))
        encoded_signature = _base64url_encode(signature)
        return f"{encoded_header}.{encoded_payload}.{encoded_signature}"

    def verify_token(self, token: str) -> Dict[str, str]:
        try:
            encoded_header, encoded_payload, encoded_signature = token.split(".")
        except ValueError as exc:
            raise AuthenticationError("无效的Token") from exc
        payload_bytes = _base64url_decode(encoded_payload)
        header_bytes = _base64url_decode(encoded_header)
        expected_signature = _base64url_encode(self._sign(f"{encoded_header}.{encoded_payload}".encode("utf-8")))
        if not hmac.compare_digest(expected_signature, encoded_signature):
            raise AuthenticationError("Token签名验证失败")
        payload = json.loads(payload_bytes.decode("utf-8"))
        header = json.loads(header_bytes.decode("utf-8"))
        if header.get("alg") != "HS256":
            raise AuthenticationError("不支持的Token算法")
        if payload.get("aud") != self.audience:
            raise AuthenticationError("Token受众不匹配")
        if int(payload.get("exp", 0)) < int(time.time()):
            raise AuthenticationError("Token已过期")
        return payload

    def _hash(self, value: str) -> str:
        return sha256(value.encode("utf-8")).hexdigest()

    def _sign(self, message: bytes) -> bytes:
        secret = self.config._config.auth.jwt_secret.encode("utf-8")
        return hmac.new(secret, message, sha256).digest()


__all__ = ["AuthManager"]
