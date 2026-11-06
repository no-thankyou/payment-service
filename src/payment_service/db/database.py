"""Database facade: the only object the transport layers (api, consumer) need from ``db``.

It owns the engine / session factory and hands out ports: a ``UnitOfWorkFactory`` for the
services and a ``ping`` for health checks. SQLAlchemy types do not leak outside ``db/``.
"""

from typing import Self

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.db.session import create_engine, create_session_factory, ping
from payment_service.db.uow import make_uow_factory
from payment_service.domain.repositories import UnitOfWorkFactory


class Database:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        engine: AsyncEngine | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._engine = engine  # set only when this object created (and so must dispose) it

    @classmethod
    def from_url(cls, url: str) -> Self:
        engine = create_engine(url)
        return cls(create_session_factory(engine), engine)

    @classmethod
    def from_engine(cls, engine: AsyncEngine) -> Self:
        """Wrap an externally owned engine (it is not disposed by ``dispose``)."""
        return cls(create_session_factory(engine))

    @property
    def uow_factory(self) -> UnitOfWorkFactory:
        return make_uow_factory(self._session_factory)

    async def ping(self) -> None:
        await ping(self._session_factory)

    async def dispose(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
