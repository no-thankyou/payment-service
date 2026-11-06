"""Unauthenticated liveness/readiness probe."""

import logging

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from payment_service.api.deps import HealthCheckDep
from payment_service.api.schemas import HealthStatus

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthStatus,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthStatus}},
)
async def health(check_database: HealthCheckDep) -> JSONResponse:
    try:
        await check_database()
    except Exception:
        logger.warning("health check: database unavailable", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"status": "unavailable"}
        )
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok"})
