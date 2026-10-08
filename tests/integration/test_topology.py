import asyncio
import time
from collections.abc import AsyncIterator

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage, AbstractRobustConnection
from faststream.rabbit import RabbitBroker
from pydantic import SecretStr

from payment_service.broker.factory import make_broker
from payment_service.broker.topology import (
    DLQ_QUEUE_NAME,
    NEW_QUEUE_NAME,
    NEW_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    declare_topology,
    retry_queue,
    retry_routing_key,
)
from payment_service.core.config import Settings

pytestmark = pytest.mark.integration

RETRY_DELAY_MS = 100
TIMEOUT_S = 5.0


@pytest.fixture
def settings(rabbit_url: str) -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://u:p@localhost:5432/db",  # unused: broker-only tests
        rabbit_url=rabbit_url,
        api_key=SecretStr("k"),
        retry_base_delay_ms=RETRY_DELAY_MS,
        retry_max_attempts=3,
    )


@pytest.fixture
async def broker(settings: Settings) -> AsyncIterator[RabbitBroker]:
    broker = make_broker(settings)
    await declare_topology(broker, settings)
    yield broker
    await broker.stop()


@pytest.fixture
async def channel(
    rabbit_url: str, settings: Settings, broker: RabbitBroker
) -> AsyncIterator[AbstractChannel]:
    connection: AbstractRobustConnection = await aio_pika.connect_robust(rabbit_url)
    ch = await connection.channel()
    names = [NEW_QUEUE_NAME, DLQ_QUEUE_NAME] + [
        retry_queue(d).name for d in settings.retry_delays_ms()
    ]
    for name in names:
        queue = await ch.get_queue(name, ensure=True)
        await queue.purge()
    yield ch
    await connection.close()


async def _get_one(ch: AbstractChannel, queue_name: str) -> AbstractIncomingMessage:
    queue = await ch.get_queue(queue_name, ensure=True)
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        message = await queue.get(no_ack=False, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.02)
    raise AssertionError(f"no message in {queue_name} within {TIMEOUT_S}s")


async def test_declare_topology_is_idempotent(settings: Settings, broker: RabbitBroker) -> None:
    await declare_topology(broker, settings)
    fresh = make_broker(settings)
    try:
        await declare_topology(fresh, settings)
        await declare_topology(fresh, settings)
    finally:
        await fresh.stop()


async def test_retry_queue_dead_letters_back_to_new_after_ttl(
    broker: RabbitBroker, channel: AbstractChannel
) -> None:
    started = time.monotonic()
    await broker.publish(
        b"retry-me",
        exchange=RETRY_EXCHANGE,
        routing_key=retry_routing_key(RETRY_DELAY_MS),
        persist=True,
        message_id="m-retry",
    )
    message = await _get_one(channel, NEW_QUEUE_NAME)
    elapsed = time.monotonic() - started
    await message.ack()
    assert message.body == b"retry-me"
    assert message.message_id == "m-retry"
    assert elapsed >= RETRY_DELAY_MS / 1000 * 0.9


async def test_rejected_new_message_lands_in_dlq(
    broker: RabbitBroker, channel: AbstractChannel
) -> None:
    await broker.publish(
        b"poison",
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_ROUTING_KEY,
        persist=True,
        message_id="m-dlq",
    )
    message = await _get_one(channel, NEW_QUEUE_NAME)
    await message.reject(requeue=False)

    dead = await _get_one(channel, DLQ_QUEUE_NAME)
    await dead.ack()
    assert dead.body == b"poison"
    assert dead.message_id == "m-dlq"
