from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt


def create_access_token(*, user_id: str, tenant_id: str, email: str) -> str:
    from app.core.config import settings

    now = datetime.now(UTC)
    payload: dict[str, Any] = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "email": email,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=settings.jwt_expiry_seconds)).timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def decode_access_token(token: str) -> dict[str, Any]:
    from app.core.config import settings

    return dict(jwt.decode(token, settings.jwt_secret, algorithms=["HS256"]))
