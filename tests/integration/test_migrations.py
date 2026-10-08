from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, inspect
from sqlalchemy.engine import make_url

from payment_service.db.base import Base
from payment_service.db.session import create_engine
from tests.integration.conftest import run_alembic

pytestmark = pytest.mark.integration


def _asyncpg_dsn(url: str) -> str:
    return make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)


@pytest.fixture
async def scratch_db_url(postgres_url: str) -> AsyncIterator[str]:
    """A fresh, empty database on the same server, dropped afterwards."""
    name = f"migrations_test_{uuid4().hex[:8]}"
    admin = await asyncpg.connect(_asyncpg_dsn(postgres_url))
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        yield make_url(postgres_url).set(database=name).render_as_string(hide_password=False)
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.close()


async def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        async with engine.connect() as conn:
            names: list[str] = await conn.run_sync(lambda c: inspect(c).get_table_names())
    finally:
        await engine.dispose()
    return set(names)


async def test_upgrade_downgrade_upgrade(scratch_db_url: str) -> None:
    run_alembic(scratch_db_url, "upgrade", "head")
    assert {"payments", "outbox"} <= await _table_names(scratch_db_url)

    run_alembic(scratch_db_url, "downgrade", "base")
    assert await _table_names(scratch_db_url) == {"alembic_version"}

    run_alembic(scratch_db_url, "upgrade", "head")
    assert {"payments", "outbox"} <= await _table_names(scratch_db_url)


def _diff(conn: Connection) -> list[Any]:
    ctx = MigrationContext.configure(conn, opts={"compare_type": True})
    return list(compare_metadata(ctx, Base.metadata))


async def test_models_match_migrations(migrated_db: str) -> None:
    """Equivalent of ``alembic check``: autogenerate finds nothing to do after upgrade head."""
    engine = create_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            diff = await conn.run_sync(_diff)
    finally:
        await engine.dispose()
    assert diff == []
