"""Integration fixtures for the database layer.

Design:
- ``migrated_db`` (session): applies ``alembic upgrade head`` once to the ``postgres_url`` database
  and returns its URL. Alembic's async ``env.py`` calls ``asyncio.run``, so the command runs in a
  worker thread with its own event loop, independent of pytest-asyncio's session loop.
- ``engine`` / ``session_factory`` (session): shared AsyncEngine and ``async_sessionmaker``.
- ``clean_db`` (function, opt-in): truncates all application tables *before* the test. Tests that
  touch the DB request ``session_factory`` together with ``clean_db``, e.g.
  ``async def test_x(session_factory, clean_db) -> None``. Not autouse, so tests that do not need
  Postgres never start a container.
"""

from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.db.session import create_engine, create_session_factory

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
APP_TABLES = ("payments", "outbox")


def alembic_config(url: str) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def run_alembic(url: str, action: str, revision: str) -> None:
    """Run ``alembic <action> <revision>`` in a separate thread (env.py uses asyncio.run)."""
    func = command.upgrade if action == "upgrade" else command.downgrade
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(func, alembic_config(url), revision).result()


@pytest.fixture(scope="session")
def migrated_db(postgres_url: str) -> str:
    run_alembic(postgres_url, "upgrade", "head")
    return postgres_url


@pytest.fixture(scope="session")
async def engine(migrated_db: str) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(migrated_db)
    yield eng
    await eng.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
async def clean_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {', '.join(APP_TABLES)}"))
