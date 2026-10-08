"""FastAPI dependencies: API key auth, Idempotency-Key header, app-scoped resources."""

import secrets
from typing import Annotated

from fastapi import Depends, Header, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.api.errors import ApiError
from payment_service.core.config import Settings

MAX_IDEMPOTENCY_KEY_LENGTH = 255

_api_key_header = APIKeyHeader(
    name="X-API-Key", auto_error=False, description="Static API key (`API_KEY` setting)."
)


def get_app_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    return factory


async def require_api_key(
    settings: Annotated[Settings, Depends(get_app_settings)],
    api_key: Annotated[str | None, Security(_api_key_header)],
) -> None:
    expected = settings.api_key.get_secret_value().encode("utf-8")
    if api_key is None or not secrets.compare_digest(api_key.encode("utf-8"), expected):
        raise ApiError(status.HTTP_401_UNAUTHORIZED, "unauthorized", "Missing or invalid API key")


def validate_idempotency_key(value: str | None) -> str:
    """Return the key as-is, or raise the ``400`` error."""
    if value is None or not value.strip():
        raise ApiError(
            status.HTTP_400_BAD_REQUEST,
            "idempotency_key_missing",
            "Idempotency-Key header is required",
        )
    if len(value) > MAX_IDEMPOTENCY_KEY_LENGTH or any(not 0x20 <= ord(c) <= 0x7E for c in value):
        raise ApiError(
            status.HTTP_400_BAD_REQUEST,
            "idempotency_key_invalid",
            f"Idempotency-Key must be 1..{MAX_IDEMPOTENCY_KEY_LENGTH} printable ASCII characters",
        )
    return value


_IDEMPOTENCY_KEY_DESCRIPTION = (
    "Required. 1..255 printable ASCII characters (0x20-0x7E). "
    "Repeating a request with the same key and body replays the original result."
)

#: OpenAPI description of the header. The FastAPI parameter itself is hidden from the schema
#: because it is optional in the signature (see ``idempotency_key``) but required by the contract.
IDEMPOTENCY_KEY_OPENAPI_PARAM: dict[str, object] = {
    "name": "Idempotency-Key",
    "in": "header",
    "required": True,
    "description": _IDEMPOTENCY_KEY_DESCRIPTION,
    "schema": {
        "type": "string",
        "minLength": 1,
        "maxLength": MAX_IDEMPOTENCY_KEY_LENGTH,
        "pattern": "^[\\x20-\\x7E]+$",
    },
}


async def idempotency_key(
    key: Annotated[str | None, Header(alias="Idempotency-Key", include_in_schema=False)] = None,
) -> str:
    # Optional in the signature so that a missing header yields our 400 instead of FastAPI's 422.
    return validate_idempotency_key(key)


SessionFactoryDep = Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)]
IdempotencyKeyDep = Annotated[str, Depends(idempotency_key)]
