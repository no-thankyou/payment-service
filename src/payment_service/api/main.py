"""FastAPI application factory.

Run with ``uvicorn --factory payment_service.api.main:app_factory``.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.api.deps import require_api_key
from payment_service.api.errors import register_exception_handlers
from payment_service.api.routes import health, payments
from payment_service.core.config import Settings, get_settings
from payment_service.core.logging import configure_logging
from payment_service.db.session import create_engine, create_session_factory


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    """Build the app. Tests inject ``session_factory``; otherwise the lifespan owns the engine."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if session_factory is not None:
            app.state.session_factory = session_factory
            yield
            return
        engine = create_engine(settings.database_url)
        app.state.session_factory = create_session_factory(engine)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="payment-service", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    if session_factory is not None:
        # Available even when the ASGI lifespan is not run (e.g. httpx ASGITransport in tests).
        app.state.session_factory = session_factory

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
