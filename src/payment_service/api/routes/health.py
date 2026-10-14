"""Unauthenticated liveness/readiness probe."""

import logging

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from payment_service.api.deps import SessionFactoryDep
from payment_service.api.schemas import HealthStatus
from payment_service.db.session import ping

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthStatus,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthStatus}},
)
async def health(session_factory: SessionFactoryDep) -> JSONResponse:
    try:
        await ping(session_factory)
    except Exception:
        logger.warning("health check: database unavailable", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"status": "unavailable"}
        )
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok"})
