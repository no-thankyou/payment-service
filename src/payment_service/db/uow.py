"""SQLAlchemy ``UnitOfWork``: one ``AsyncSession`` = one transaction shared by the repositories."""

from functools import partial
from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.repositories import SqlAlchemyOutboxRepository, SqlAlchemyPaymentRepository
from payment_service.domain.repositories import UnitOfWorkFactory


class SqlAlchemyUnitOfWork:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        # The session connects and begins its transaction lazily, on the first statement.
        self._session = session_factory()
        self.payments = SqlAlchemyPaymentRepository(self._session)
        self.outbox = SqlAlchemyOutboxRepository(self._session)

    async def commit(self) -> None:
        await self._session.commit()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Closing rolls back whatever was not committed and returns the connection to the pool.
        await self._session.close()


def make_uow_factory(session_factory: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
    return partial(SqlAlchemyUnitOfWork, session_factory)
