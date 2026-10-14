import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker.topology import NEW_ROUTING_KEY, PAYMENTS_EXCHANGE_NAME
from payment_service.db.models import OutboxEvent, Payment
from payment_service.db.uow import make_uow_factory
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError
from payment_service.domain.events import PAYMENT_CREATED
from payment_service.domain.models import NewPayment
from payment_service.services.payments import PaymentService, compute_request_hash

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_db")]

SessionFactory = async_sessionmaker[AsyncSession]


@pytest.fixture
def service(session_factory: SessionFactory) -> PaymentService:
    return PaymentService(make_uow_factory(session_factory))


def _data(amount: str = "100.50") -> NewPayment:
    return NewPayment(
        amount=Decimal(amount),
        currency=Currency.RUB,
        description="Order #1234",
        metadata={
            "order_id": "1234",
            "customer_id": 42,
            "nested": {"tags": ["a", "\u0437\u0430\u043a\u0430\u0437"]},
        },
        webhook_url="https://merchant.example.com/hooks/payments",
    )


async def _count(session_factory: SessionFactory, model: type[Payment | OutboxEvent]) -> int:
    async with session_factory() as session:
        return (await session.scalar(select(func.count()).select_from(model))) or 0


async def _outbox(session_factory: SessionFactory) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


async def test_create_inserts_payment_and_outbox(
    service: PaymentService, session_factory: SessionFactory
) -> None:
    data = _data()
    result = await service.create("key-1", data)

    assert result.replayed is False
    p = result.payment
    assert p.status is PaymentStatus.PENDING
    assert p.idempotency_key == "key-1"
    assert p.request_hash == compute_request_hash(data)
    assert p.amount == Decimal("100.50")
    assert p.currency is Currency.RUB
    assert p.created_at.tzinfo is not None
    assert p.webhook_attempts == 0
    assert p.processed_at is None

    events = await _outbox(session_factory)
    assert len(events) == 1
    event = events[0]
    assert event.event_type == PAYMENT_CREATED
    assert event.exchange == PAYMENTS_EXCHANGE_NAME
    assert event.routing_key == NEW_ROUTING_KEY
    assert event.published_at is None
    assert UUID(event.payload["payment_id"]) == p.id
    assert UUID(event.payload["event_id"]) == event.id


async def test_replay_same_body_returns_existing(
    service: PaymentService, session_factory: SessionFactory
) -> None:
    first = await service.create("key-1", _data())
    second = await service.create("key-1", _data("100.5"))

    assert second.replayed is True
    assert second.payment.id == first.payment.id
    assert second.payment.created_at == first.payment.created_at
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_same_key_different_body_rejected(
    service: PaymentService, session_factory: SessionFactory
) -> None:
    await service.create("key-1", _data())
    with pytest.raises(IdempotencyKeyReusedError):
        await service.create("key-1", _data("200.00"))
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_different_keys_create_separate_payments(
    service: PaymentService, session_factory: SessionFactory
) -> None:
    a = await service.create("key-a", _data())
    b = await service.create("key-b", _data())
    assert a.payment.id != b.payment.id
    assert await _count(session_factory, OutboxEvent) == 2


async def test_concurrent_creates_with_same_key(
    service: PaymentService, session_factory: SessionFactory
) -> None:
    results = await asyncio.gather(*(service.create("race-key", _data()) for _ in range(10)))

    assert len({r.payment.id for r in results}) == 1
    assert sum(not r.replayed for r in results) == 1
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_get_payment_roundtrip(
    service: PaymentService, session_factory: SessionFactory
) -> None:
    data = _data()
    created = await service.create("key-1", data)

    loaded = await service.get(created.payment.id)
    assert loaded.id == created.payment.id
    assert loaded.metadata == data.metadata
    assert loaded.webhook_url == data.webhook_url
    assert loaded.description == data.description
    assert loaded.status is PaymentStatus.PENDING


async def test_get_payment_unknown(service: PaymentService) -> None:
    unknown = uuid4()
    with pytest.raises(PaymentNotFoundError) as exc_info:
        await service.get(unknown)
    assert exc_info.value.payment_id == unknown
