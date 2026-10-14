"""Async engine and session factory."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine


def create_engine(url: str) -> AsyncEngine:
    return _create_async_engine(url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Round-trip to the database; raises if it is unreachable."""
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))
