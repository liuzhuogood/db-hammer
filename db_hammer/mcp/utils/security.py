"""Security helpers for the MCP server."""
from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional

try:  # pragma: no cover
    from jose import JWTError, jwt
except ImportError:  # pragma: no cover
    JWTError = Exception  # type: ignore

    class _StubJWT:
        def encode(self, *args, **kwargs):  # type: ignore[override]
            raise AuthorizationError("python-jose 未安装，无法生成令牌")

        def decode(self, *args, **kwargs):  # type: ignore[override]
            raise AuthorizationError("python-jose 未安装，无法验证令牌")

    jwt = _StubJWT()  # type: ignore

from ..config import MCPConfig, SecurityConfig
from ..exceptions import AuthorizationError


class SecurityManager:
    """Provide basic API key and JWT token based authentication."""

    def __init__(self, config: MCPConfig):
        self._config = config
        self._api_keys = set(config.security.default_api_keys)
        for provider in config.security.providers:
            if provider.provider == "api_key":
                self._api_keys.update(provider.options.get("keys", []))

    @property
    def config(self) -> SecurityConfig:
        return self._config.security

    def verify_api_key(self, api_key: Optional[str]) -> None:
        """Verify API key when API key providers are enabled."""

        if not self.config.enabled:
            return
        if not self._api_keys:
            raise AuthorizationError("API key authentication not configured")
        if not api_key:
            raise AuthorizationError("Missing API key")
        if api_key not in self._api_keys:
            raise AuthorizationError("Invalid API key")

    def issue_token(self, subject: str, expires_in: int = 3600) -> str:
        """Generate a JWT for the provided subject."""

        jwt_provider = self._get_jwt_provider()
        secret = jwt_provider.get("secret", secrets.token_hex(32))
        algorithm = jwt_provider.get("algorithm", "HS256")
        now = datetime.now(timezone.utc)
        payload = {
            "sub": subject,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
        }
        return jwt.encode(payload, secret, algorithm=algorithm)

    def verify_token(self, token: Optional[str]) -> Dict[str, str]:
        """Verify an incoming JWT."""

        if not self.config.enabled:
            return {"sub": "anonymous"}
        if not token:
            raise AuthorizationError("Missing authorization token")
        jwt_provider = self._get_jwt_provider()
        secret = jwt_provider.get("secret")
        algorithm = jwt_provider.get("algorithm", "HS256")
        if not secret:
            raise AuthorizationError("JWT secret not configured")
        try:
            return jwt.decode(token, secret, algorithms=[algorithm])
        except JWTError as exc:
            raise AuthorizationError(f"Invalid token: {exc}") from exc

    def _get_jwt_provider(self) -> Dict[str, str]:
        for provider in self.config.providers:
            if provider.provider == "jwt":
                return provider.options
        return {}

    def refresh_api_keys(self, api_keys: Iterable[str]) -> None:
        self._api_keys = set(api_keys)


__all__ = ["SecurityManager"]
