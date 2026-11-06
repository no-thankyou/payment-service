"""FastAPI application factory.

Run with ``uvicorn --factory payment_service.api.main:app_factory``.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI

from payment_service.api.deps import require_api_key
from payment_service.api.errors import register_exception_handlers
from payment_service.api.routes import health, payments
from payment_service.core.config import Settings, get_settings
from payment_service.core.logging import configure_logging
from payment_service.db.database import Database
from payment_service.services.payments import PaymentService


def _init_state(app: FastAPI, database: Database) -> None:
    """Composition root of the API: wire the services onto the data-access layer."""
    app.state.health_check = database.ping
    app.state.payment_service = PaymentService(database.uow_factory)


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    """Build the app. Tests inject ``database``; otherwise the lifespan owns it."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if database is not None:
            _init_state(app, database)
            yield
            return
        own_database = Database.from_url(settings.database_url)
        _init_state(app, own_database)
        try:
            yield
        finally:
            await own_database.dispose()

    app = FastAPI(title="payment-service", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    if database is not None:
        # Available even when the ASGI lifespan is not run (e.g. httpx ASGITransport in tests).
        _init_state(app, database)

    api_v1 = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
    api_v1.include_router(payments.router)
    app.include_router(api_v1)
    app.include_router(health.router)
    register_exception_handlers(app)
    return app


def app_factory() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    return create_app(settings)
