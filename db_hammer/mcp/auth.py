"""Authentication helpers for the db-hammer MCP server."""

from __future__ import annotations

import hmac
import logging
from hashlib import sha256
from typing import Dict, Iterable, Optional

try:  # pragma: no cover - optional dependency
    from jose import jwt
except Exception:  # pragma: no cover - the package is optional for tests
    jwt = None  # type: ignore

from .config import AuthenticationProvider
from .exceptions import AuthenticationError
from .utils.security import hash_token

LOGGER = logging.getLogger(__name__)


class AuthManager:
    """Authenticate requests using API keys or JWT tokens."""

    def __init__(self, providers: Iterable[AuthenticationProvider]) -> None:
        self._providers = list(providers)
        self._hashed_keys: Dict[str, str] = {}
        for provider in self._providers:
            for key in provider.api_keys:
                self._hashed_keys[hash_token(key)] = provider.name

    def authenticate_api_key(self, provided_key: str) -> str:
        """Validate the provided API key and return the provider name."""

        hashed = hash_token(provided_key)
        for expected, provider in self._hashed_keys.items():
            if hmac.compare_digest(expected, hashed):
                LOGGER.debug("API key authenticated via provider %s", provider)
                return provider
        raise AuthenticationError("Invalid API key")

    def authenticate_jwt(self, token: str) -> str:
        """Validate the given JWT token using the configured providers."""

        if not jwt:
            raise AuthenticationError("python-jose is not installed; JWT auth unavailable")

        for provider in self._providers:
            if not provider.jwk:
                continue
            try:
                payload = jwt.decode(
                    token,
                    provider.jwk,
                    algorithms=provider.jwk.get("alg", "HS256"),
                    audience=provider.audience,
                    issuer=provider.issuer,
                )
            except Exception as exc:  # pragma: no cover - external dependency
                LOGGER.warning("JWT validation failed for provider %s: %s", provider.name, exc)
                continue
            subject = payload.get("sub", provider.name)
            LOGGER.debug("JWT authenticated for subject %s via %s", subject, provider.name)
            return subject
        raise AuthenticationError("Unable to validate JWT token")

    def has_providers(self) -> bool:
        return bool(self._providers)

    def require_authentication(self) -> bool:
        return self.has_providers()

    def verify(self, api_key: Optional[str] = None, token: Optional[str] = None) -> str:
        if api_key:
            return self.authenticate_api_key(api_key)
        if token:
            return self.authenticate_jwt(token)
        if self.require_authentication():
            raise AuthenticationError("Authentication required")
        return "anonymous"
