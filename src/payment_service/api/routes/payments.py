"""Payment endpoints.

The request body is parsed inside the handler (not as a typed FastAPI body parameter) so that the
check precedence 401 → 400 → 415/413 → 422 holds for every input: FastAPI decodes a typed JSON
body *before* resolving dependencies, so malformed JSON would otherwise produce ``422`` ahead of
``401``/``400``.
For the same reason the ``415`` (Content-Type) and ``413`` (body size) checks run here, after the
auth / Idempotency-Key dependencies and before the body is read.
"""

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Path, Request, Response, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from payment_service.api.deps import (
    IDEMPOTENCY_KEY_OPENAPI_PARAM,
    IdempotencyKeyDep,
    SessionFactoryDep,
)
from payment_service.api.errors import ApiError
from payment_service.api.schemas import ErrorBody, PaymentAccepted, PaymentCreate, PaymentRead
from payment_service.services.payments import create_payment, get_payment

REPLAYED_HEADER = "Idempotent-Replayed"
MAX_BODY_BYTES = 65536

router = APIRouter(prefix="/payments", tags=["payments"])


def _payment_create_request_body() -> dict[str, Any]:
    schema = PaymentCreate.model_json_schema(ref_template="#/components/schemas/{model}")
    # Nested models (Currency) are already registered in components via the response models.
    schema.pop("$defs", None)
    return {
        "required": True,
        "content": {"application/json": {"schema": schema}},
    }


def _payload_too_large() -> ApiError:
    return ApiError(
        status.HTTP_413_CONTENT_TOO_LARGE,
        "payload_too_large",
        f"Request body must not exceed {MAX_BODY_BYTES} bytes",
    )


def _check_content_type(request: Request) -> None:
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media_type != "application/json":
        raise ApiError(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Content-Type must be application/json",
        )


async def _read_body_limited(request: Request) -> bytes:
    """Reject by declared ``Content-Length`` first, then enforce the limit while reading."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise _payload_too_large()
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise _payload_too_large()
        chunks.append(chunk)
    return b"".join(chunks)


async def _parse_body(request: Request) -> PaymentCreate:
    _check_content_type(request)
    raw = await _read_body_limited(request)
    try:
        return PaymentCreate.model_validate_json(raw)
    except ValidationError as exc:
        errors = [{**err, "loc": ("body", *err["loc"])} for err in exc.errors(include_url=False)]
        raise RequestValidationError(errors, body=raw) from exc


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Create a payment (asynchronous processing)",
    openapi_extra={
        "parameters": [IDEMPOTENCY_KEY_OPENAPI_PARAM],
        "requestBody": _payment_create_request_body(),
    },
    responses={
        status.HTTP_202_ACCEPTED: {
            "description": "Payment accepted for processing (or replayed for a repeated key).",
            "headers": {
                REPLAYED_HEADER: {
                    "description": "Present with value `true` when the response is a replay "
                    "of an earlier request with the same Idempotency-Key.",
                    "schema": {"type": "string", "enum": ["true"]},
                }
            },
        },
        status.HTTP_400_BAD_REQUEST: {
            "model": ErrorBody,
            "description": "`idempotency_key_missing` / `idempotency_key_invalid`",
        },
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorBody, "description": "`unauthorized`"},
        status.HTTP_413_CONTENT_TOO_LARGE: {
            "model": ErrorBody,
            "description": f"`payload_too_large` (body > {MAX_BODY_BYTES} bytes)",
        },
        status.HTTP_415_UNSUPPORTED_MEDIA_TYPE: {
            "model": ErrorBody,
            "description": "`unsupported_media_type` (Content-Type is not application/json)",
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Body validation error (FastAPI format) or `idempotency_key_reused` "
            "(application error body).",
        },
    },
)
async def create_payment_endpoint(
    request: Request,
    response: Response,
    key: IdempotencyKeyDep,
    session_factory: SessionFactoryDep,
) -> PaymentAccepted:
    payload = await _parse_body(request)
    result = await create_payment(session_factory, key, payload.to_new_payment())
    if result.replayed:
        response.headers[REPLAYED_HEADER] = "true"
    return PaymentAccepted.from_model(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get a payment",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorBody, "description": "`unauthorized`"},
        status.HTTP_404_NOT_FOUND: {"model": ErrorBody, "description": "`payment_not_found`"},
    },
)
async def get_payment_endpoint(
    payment_id: Annotated[UUID, Path()],
    session_factory: SessionFactoryDep,
) -> PaymentRead:
    return PaymentRead.from_model(await get_payment(session_factory, payment_id))
