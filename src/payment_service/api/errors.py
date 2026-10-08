"""Application error helpers and exception handlers."""

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError


class ApiError(HTTPException):
    """HTTPException rendered as ``{"detail": {"code": ..., "message": ...}}``."""

    def __init__(
        self, status_code: int, code: str, message: str, headers: dict[str, str] | None = None
    ) -> None:
        super().__init__(
            status_code=status_code, detail={"code": code, "message": message}, headers=headers
        )
        self.code = code


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code, content={"detail": {"code": code, "message": message}}
    )


async def _idempotency_key_reused(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "idempotency_key_reused",
        "Idempotency-Key was already used with a different request body",
    )


async def _payment_not_found(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(status.HTTP_404_NOT_FOUND, "payment_not_found", "Payment not found")


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(IdempotencyKeyReusedError, _idempotency_key_reused)
    app.add_exception_handler(PaymentNotFoundError, _payment_not_found)
