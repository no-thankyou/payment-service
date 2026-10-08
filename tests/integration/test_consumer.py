"""Consumer app end-to-end on real PostgreSQL + RabbitMQ.

The app is started in-process with ``FastStream.start()`` / ``stop()`` (the hooks + broker start
that ``FastStream.run`` performs, without signal handling). ``TestRabbitBroker`` is not used: it
does not emulate TTL / dead-lettering, which is what the retry and DLQ paths rely on.
"""

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import aio_pika
import httpx
import pytest
import respx
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage, AbstractQueue
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker.factory import make_broker
from payment_service.broker.topology import (
    DLQ_QUEUE_NAME,
    NEW_QUEUE_NAME,
    NEW_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    declare_topology,
    retry_queue,
)
from payment_service.consumer.main import create_consumer
from payment_service.core.config import Settings
from payment_service.db.models import Payment
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.services.payments import NewPayment, create_payment

pytestmark = pytest.mark.integration

SessionFactory = async_sessionmaker[AsyncSession]
WEBHOOK_URL = "https://merchant.example.com/hooks/payments"
TIMEOUT_S = 10.0


class CountingGateway:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> PaymentStatus:
        self.calls.append(payment_id)
        await asyncio.sleep(0)
        return PaymentStatus.SUCCEEDED


@pytest.fixture
def settings(migrated_db: str, rabbit_url: str) -> Settings:
    return Settings(
        database_url=migrated_db,
        rabbit_url=rabbit_url,
        api_key=SecretStr("k"),
        retry_base_delay_ms=100,
        retry_max_attempts=3,
        gateway_min_delay_s=0,
        gateway_max_delay_s=0,
        gateway_success_rate=1.0,
        outbox_poll_interval_s=0.1,
        webhook_timeout_s=1.0,
    )


@pytest.fixture
async def channel(settings: Settings) -> AsyncIterator[AbstractChannel]:
    """aio-pika channel for inspecting queues; all queues are declared and purged first."""
    broker = make_broker(settings)
    try:
        await declare_topology(broker, settings)
    finally:
        await broker.stop()
    connection = await aio_pika.connect_robust(settings.rabbit_url)
    ch = await connection.channel()
    names = [NEW_QUEUE_NAME, DLQ_QUEUE_NAME] + [
        retry_queue(d).name for d in settings.retry_delays_ms()
    ]
    for name in names:
        await (await ch.get_queue(name, ensure=True)).purge()
    yield ch
    for name in names:
        await (await ch.get_queue(name, ensure=True)).purge()
    await connection.close()


@pytest.fixture
def mock_http() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as router:
        yield router


@pytest.fixture
def gateway() -> CountingGateway:
    return CountingGateway()


@pytest.fixture
async def consumer(
    settings: Settings,
    session_factory: SessionFactory,
    clean_db: None,
    channel: AbstractChannel,
    gateway: CountingGateway,
    mock_http: respx.MockRouter,
) -> AsyncIterator[FastStream]:
    async with httpx.AsyncClient() as client:
        app = create_consumer(
            settings, session_factory=session_factory, gateway=gateway, http_client=client
        )
        await app.start()
        try:
            yield app
        finally:
            await app.stop()


@pytest.fixture
async def publisher(settings: Settings) -> AsyncIterator[RabbitBroker]:
    broker = make_broker(settings)
    await broker.connect()
    yield broker
    await broker.stop()


async def _create(session_factory: SessionFactory) -> UUID:
    result = await create_payment(
        session_factory,
        f"key-{uuid4()}",
        NewPayment(
            amount=Decimal("10.50"),
            currency=Currency.USD,
            description="consumer test",
            metadata={"order_id": "1"},
            webhook_url=WEBHOOK_URL,
        ),
    )
    return result.payment.id


async def _get(session_factory: SessionFactory, payment_id: UUID) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


async def _wait_for(predicate: Callable[[], Awaitable[bool]], what: str) -> None:
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out after {TIMEOUT_S}s waiting for {what}")


async def _wait_delivered(session_factory: SessionFactory, payment_id: UUID) -> Payment:
    async def delivered() -> bool:
        return (await _get(session_factory, payment_id)).webhook_delivered_at is not None

    await _wait_for(delivered, f"webhook delivery of {payment_id}")
    return await _get(session_factory, payment_id)


async def _get_one(ch: AbstractChannel, queue_name: str) -> AbstractIncomingMessage:
    queue = await ch.get_queue(queue_name, ensure=True)
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        message = await queue.get(no_ack=False, fail=False)
        if message is not None:
            await message.ack()
            return message
        await asyncio.sleep(0.05)
    raise AssertionError(f"no message in {queue_name} within {TIMEOUT_S}s")


async def _drain(ch: AbstractChannel, queue_name: str) -> list[AbstractIncomingMessage]:
    queue: AbstractQueue = await ch.get_queue(queue_name, ensure=True)
    messages: list[AbstractIncomingMessage] = []
    while (message := await queue.get(no_ack=True, fail=False)) is not None:
        messages.append(message)
    return messages


async def test_happy_path_outbox_to_webhook(
    consumer: FastStream,
    session_factory: SessionFactory,
    channel: AbstractChannel,
    gateway: CountingGateway,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _create(session_factory)

    payment = await _wait_delivered(session_factory, payment_id)

    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_attempts == 1
    assert gateway.calls == [payment_id]
    assert route.call_count == 1
    assert json.loads(route.calls[0].request.content)["payment_id"] == str(payment_id)
    await asyncio.sleep(0.2)
    assert await _drain(channel, DLQ_QUEUE_NAME) == []


async def test_webhook_always_failing_ends_in_dlq_after_max_attempts(
    consumer: FastStream,
    session_factory: SessionFactory,
    channel: AbstractChannel,
    gateway: CountingGateway,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(500))
    started = time.monotonic()
    payment_id = await _create(session_factory)

    dead = await _get_one(channel, DLQ_QUEUE_NAME)
    elapsed = time.monotonic() - started

    assert elapsed >= 0.3  # 100 ms + 200 ms of retry delays
    assert dead.headers["x-attempt"] == 3
    assert "HTTP 500" in str(dead.headers["x-last-error"])
    assert json.loads(dead.body)["payment_id"] == str(payment_id)
    payment = await _get(session_factory, payment_id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_attempts == 3
    assert payment.webhook_delivered_at is None
    assert gateway.calls == [payment_id]
    assert route.call_count == 3


async def test_webhook_fails_once_then_delivered(
    consumer: FastStream,
    session_factory: SessionFactory,
    channel: AbstractChannel,
    gateway: CountingGateway,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(side_effect=[httpx.Response(500), httpx.Response(200)])
    payment_id = await _create(session_factory)

    payment = await _wait_delivered(session_factory, payment_id)

    assert payment.webhook_attempts == 2
    assert gateway.calls == [payment_id]
    assert route.call_count == 2
    await asyncio.sleep(0.2)
    assert await _drain(channel, DLQ_QUEUE_NAME) == []


@pytest.mark.parametrize(
    "body",
    [b"{not json", json.dumps({"payment_id": "not-a-uuid"}).encode(), b"{}"],
    ids=["invalid-json", "invalid-uuid", "missing-field"],
)
async def test_malformed_message_goes_to_dlq_and_consumer_keeps_working(
    body: bytes,
    consumer: FastStream,
    publisher: RabbitBroker,
    session_factory: SessionFactory,
    channel: AbstractChannel,
    mock_http: respx.MockRouter,
) -> None:
    mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    await publisher.publish(
        body,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_ROUTING_KEY,
        content_type="application/json",
        message_id="malformed",
        persist=True,
    )

    dead = await _get_one(channel, DLQ_QUEUE_NAME)
    assert dead.body == body
    assert dead.message_id == "malformed"
    assert "x-attempt" not in (dead.headers or {})

    payment_id = await _create(session_factory)
    payment = await _wait_delivered(session_factory, payment_id)
    assert payment.webhook_attempts == 1


async def test_event_without_event_id_and_occurred_at_goes_to_dlq(
    consumer: FastStream,
    publisher: RabbitBroker,
    session_factory: SessionFactory,
    channel: AbstractChannel,
    gateway: CountingGateway,
) -> None:
    # Payment inserted without an outbox row, so the only event for it is the malformed one.
    payment_id = uuid4()
    async with session_factory.begin() as session:
        session.add(
            Payment(
                id=payment_id,
                idempotency_key=f"key-{payment_id}",
                request_hash="0" * 64,
                amount=Decimal("10.50"),
                currency=Currency.USD,
                description="consumer test",
                metadata_={},
                status=PaymentStatus.PENDING,
                webhook_url=WEBHOOK_URL,
            )
        )
    body = json.dumps({"payment_id": str(payment_id)}).encode()
    await publisher.publish(
        body,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_ROUTING_KEY,
        content_type="application/json",
        message_id="only-payment-id",
        persist=True,
    )

    dead = await _get_one(channel, DLQ_QUEUE_NAME)

    assert dead.body == body
    assert gateway.calls == []
    assert (await _get(session_factory, payment_id)).status == PaymentStatus.PENDING


async def test_unknown_payment_goes_to_dlq_without_retries(
    consumer: FastStream,
    publisher: RabbitBroker,
    channel: AbstractChannel,
    gateway: CountingGateway,
) -> None:
    event = PaymentCreatedEvent(event_id=uuid4(), payment_id=uuid4(), occurred_at=datetime.now(UTC))
    await publisher.publish(
        event.model_dump(mode="json"),
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_ROUTING_KEY,
        message_id=str(event.event_id),
        persist=True,
    )

    dead = await _get_one(channel, DLQ_QUEUE_NAME)

    assert dead.message_id == str(event.event_id)
    assert dead.headers.get("x-attempt", 1) == 1
    assert gateway.calls == []
    await asyncio.sleep(0.4)  # longer than all retry delays: nothing comes back
    assert await _drain(channel, DLQ_QUEUE_NAME) == []
    assert await _drain(channel, NEW_QUEUE_NAME) == []


async def test_failed_retry_publish_dead_letters_original(
    settings: Settings,
    consumer: FastStream,
    publisher: RabbitBroker,
    session_factory: SessionFactory,
    channel: AbstractChannel,
    gateway: CountingGateway,
    mock_http: respx.MockRouter,
) -> None:
    # Without the first delay queue the retry copy is unroutable (returned to the publisher).
    first_retry_queue = retry_queue(settings.retry_delays_ms()[0]).name
    await channel.queue_delete(first_retry_queue)
    try:
        route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(500))
        payment_id = await _create(session_factory)

        dead = await _get_one(channel, DLQ_QUEUE_NAME)

        assert json.loads(dead.body)["payment_id"] == str(payment_id)
        assert dead.headers.get("x-attempt", 1) == 1
        await asyncio.sleep(0.4)  # no immediate redelivery loop, nothing else dead-lettered
        assert await _drain(channel, DLQ_QUEUE_NAME) == []
        assert await _drain(channel, NEW_QUEUE_NAME) == []
        payment = await _get(session_factory, payment_id)
        assert payment.webhook_attempts == 1
        assert payment.webhook_delivered_at is None
        assert route.call_count == 1
        assert gateway.calls == [payment_id]
    finally:
        await declare_topology(publisher, settings)
