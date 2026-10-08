import aio_pika
import asyncpg
import pytest

pytestmark = pytest.mark.integration


async def test_postgres_reachable(postgres_url: str) -> None:
    conn = await asyncpg.connect(postgres_url.replace("postgresql+asyncpg", "postgresql"))
    try:
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


async def test_rabbit_reachable(rabbit_url: str) -> None:
    conn = await aio_pika.connect(rabbit_url)
    await conn.close()
