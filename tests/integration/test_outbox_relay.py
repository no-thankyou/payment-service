import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage, AbstractQueue
from faststream.rabbit import RabbitBroker
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker.factory import make_broker
from payment_service.broker.topology import (
    NEW_QUEUE_NAME,
    NEW_ROUTING_KEY,
    PAYMENTS_EXCHANGE_NAME,
    declare_topology,
)
from payment_service.core.config import Settings
from payment_service.db.models import OutboxEvent
from payment_service.db.uow import make_uow_factory
from payment_service.outbox.relay import OutboxRelay

pytestmark = pytest.mark.integration

SessionFactory = async_sessionmaker[AsyncSession]
TIMEOUT_S = 5.0


@pytest.fixture
def settings(postgres_url: str, rabbit_url: str) -> Settings:
    return Settings(database_url=postgres_url, rabbit_url=rabbit_url, api_key=SecretStr("k"))


@pytest.fixture
async def broker(settings: Settings) -> AsyncIterator[RabbitBroker]:
    broker = make_broker(settings)
    await declare_topology(broker, settings)
    yield broker
    await broker.stop()


@pytest.fixture
async def new_queue(rabbit_url: str, broker: RabbitBroker) -> AsyncIterator[AbstractQueue]:
    """``payments.new`` (purged) read directly via aio-pika."""
    connection = await aio_pika.connect_robust(rabbit_url)
    channel: AbstractChannel = await connection.channel()
    queue = await channel.get_queue(NEW_QUEUE_NAME, ensure=True)
    await queue.purge()
    yield queue
    await queue.purge()
    await connection.close()


async def _insert_events(
    session_factory: SessionFactory,
    count: int,
    *,
    routing_key: str = NEW_ROUTING_KEY,
    published: bool = False,
) -> list[OutboxEvent]:
    base = datetime.now(UTC) - timedelta(minutes=1)
    rows = [
        OutboxEvent(
            id=uuid.uuid4(),
            event_type="payment.created",
            exchange=PAYMENTS_EXCHANGE_NAME,
            routing_key=routing_key,
            payload={"payment_id": str(uuid.uuid4()), "seq": i},
            created_at=base + timedelta(milliseconds=i),
            published_at=datetime.now(UTC) if published else None,
        )
        for i in range(count)
    ]
    async with session_factory() as session, session.begin():
        session.add_all(rows)
    return rows


async def _load(
    session_factory: SessionFactory, ids: list[uuid.UUID]
) -> dict[uuid.UUID, OutboxEvent]:
    async with session_factory() as session:
        result = await session.scalars(select(OutboxEvent).where(OutboxEvent.id.in_(ids)))
        return {row.id: row for row in result}


async def _get_message(queue: AbstractQueue) -> AbstractIncomingMessage:
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        message = await queue.get(no_ack=False, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.02)
    raise AssertionError(f"no message in {queue.name} within {TIMEOUT_S}s")


async def _drain(queue: AbstractQueue) -> list[AbstractIncomingMessage]:
    messages: list[AbstractIncomingMessage] = []
    while (message := await queue.get(no_ack=True, fail=False)) is not None:
        messages.append(message)
    return messages


def _relay(
    session_factory: SessionFactory,
    broker: RabbitBroker,
    batch_size: int = 100,
    poll_interval_s: float = 0.05,
) -> OutboxRelay:
    return OutboxRelay(make_uow_factory(session_factory), broker, batch_size, poll_interval_s)


async def test_run_once_publishes_in_created_at_order(
    session_factory: SessionFactory,
    clean_db: None,
    broker: RabbitBroker,
    new_queue: AbstractQueue,
) -> None:
    rows = await _insert_events(session_factory, 3)

    assert await _relay(session_factory, broker).run_once() == 3

    stored = await _load(session_factory, [r.id for r in rows])
    assert all(stored[r.id].published_at is not None for r in rows)
    for row in rows:
        message = await _get_message(new_queue)
        await message.ack()
        assert message.message_id == str(row.id)
        assert json.loads(message.body) == row.payload
        assert message.content_type == "application/json"
        assert message.headers["x-event-type"] == "payment.created"
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert await _drain(new_queue) == []


async def test_already_published_rows_are_skipped(
    session_factory: SessionFactory,
    clean_db: None,
    broker: RabbitBroker,
    new_queue: AbstractQueue,
) -> None:
    await _insert_events(session_factory, 2, published=True)
    relay = _relay(session_factory, broker)

    assert await relay.run_once() == 0

    await _insert_events(session_factory, 1)
    assert await relay.run_once() == 1
    assert await relay.run_once() == 0
    await _get_message(new_queue)
    assert await _drain(new_queue) == []


async def test_concurrent_relays_do_not_double_publish(
    settings: Settings,
    session_factory: SessionFactory,
    clean_db: None,
    broker: RabbitBroker,
    new_queue: AbstractQueue,
) -> None:
    rows = await _insert_events(session_factory, 50)
    other_broker = make_broker(settings)
    await other_broker.connect()
    try:
        relay_a = _relay(session_factory, broker, batch_size=10)
        relay_b = _relay(session_factory, other_broker, batch_size=10)
        total = 0
        for _ in range(10):
            a, b = await asyncio.gather(relay_a.run_once(), relay_b.run_once())
            total += a + b
            if a == b == 0:
                break
    finally:
        await other_broker.stop()

    assert total == 50
    stored = await _load(session_factory, [r.id for r in rows])
    assert all(row.published_at is not None for row in stored.values())
    await asyncio.sleep(0.2)
    messages = await _drain(new_queue)
    assert len(messages) == 50
    assert {m.message_id for m in messages} == {str(r.id) for r in rows}


class _FailingBroker:
    """Stub whose publish fails on the N-th call (1-based)."""

    def __init__(self, fail_on: int) -> None:
        self.fail_on = fail_on
        self.calls: list[str | None] = []

    async def publish(self, message: Any, **kwargs: Any) -> None:
        self.calls.append(kwargs.get("message_id"))
        if len(self.calls) == self.fail_on:
            raise ConnectionError("broker unavailable")


async def test_publish_failure_stops_batch_and_records_error(
    session_factory: SessionFactory, clean_db: None
) -> None:
    rows = await _insert_events(session_factory, 3)
    stub = _FailingBroker(fail_on=2)

    published = await _relay(session_factory, cast(RabbitBroker, stub)).run_once()

    assert published == 1
    assert stub.calls == [str(rows[0].id), str(rows[1].id)]
    stored = await _load(session_factory, [r.id for r in rows])
    first, second, third = (stored[r.id] for r in rows)
    assert first.published_at is not None
    assert second.published_at is None
    assert second.attempts == 1
    assert second.last_error == "broker unavailable"
    assert third.published_at is None
    assert third.attempts == 0
    assert third.last_error is None


async def test_unroutable_message_is_a_publish_failure(
    session_factory: SessionFactory,
    clean_db: None,
    broker: RabbitBroker,
    new_queue: AbstractQueue,
) -> None:
    (row,) = await _insert_events(session_factory, 1, routing_key="no.such.binding")

    assert await _relay(session_factory, broker).run_once() == 0

    stored = (await _load(session_factory, [row.id]))[row.id]
    assert stored.published_at is None
    assert stored.attempts == 1
    assert stored.last_error is not None
    assert "returned" in stored.last_error


async def test_run_forever_publishes_and_stops(
    session_factory: SessionFactory,
    clean_db: None,
    broker: RabbitBroker,
    new_queue: AbstractQueue,
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(
        _relay(session_factory, broker, poll_interval_s=0.05).run_forever(stop)
    )
    try:
        (row,) = await _insert_events(session_factory, 1)
        message = await asyncio.wait_for(_get_message(new_queue), timeout=2.0)
        await message.ack()
        assert message.message_id == str(row.id)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
    assert task.done()
    assert task.exception() is None
    stored = (await _load(session_factory, [row.id]))[row.id]
    assert stored.published_at is not None
