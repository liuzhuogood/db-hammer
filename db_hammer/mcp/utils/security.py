"""Security helpers for MCP server."""
from __future__ import annotations

import hmac
import logging
from hashlib import sha256
from typing import Optional

from ..exceptions import AuthenticationError

LOGGER = logging.getLogger(__name__)


def validate_bearer_token(provided_token: Optional[str], secret: Optional[str]) -> None:
    """Validate bearer token using constant-time comparison."""

    if secret is None:
        LOGGER.debug("Token authentication disabled; skipping validation")
        return

    if not provided_token:
        raise AuthenticationError("Missing authentication token")

    expected = sha256(secret.encode("utf-8")).hexdigest()
    provided = sha256(provided_token.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(expected, provided):
        raise AuthenticationError("Invalid authentication token")
