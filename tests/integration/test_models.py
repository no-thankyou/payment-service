from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain.enums import Currency, PaymentStatus

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_db")]


def _payment(key: str = "key-1", amount: str = "100.50") -> Payment:
    return Payment(
        idempotency_key=key,
        request_hash="0" * 64,
        amount=Decimal(amount),
        currency=Currency.USD,
        description="order #1",
        metadata_={"order_id": 42, "tags": ["a", "b"]},
        webhook_url="http://example.test/hook",
    )


async def test_insert_and_read_back(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as session, session.begin():
        payment = _payment()
        session.add(payment)
    payment_id = payment.id

    async with session_factory() as session:
        loaded = await session.get(Payment, payment_id)
    assert loaded is not None
    assert loaded.amount == Decimal("100.50")
    assert loaded.currency is Currency.USD
    assert loaded.status is PaymentStatus.PENDING
    assert loaded.metadata_ == {"order_id": 42, "tags": ["a", "b"]}
    assert loaded.webhook_attempts == 0
    assert loaded.created_at.tzinfo is not None
    assert loaded.processed_at is None


async def test_amount_must_be_positive(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        session.add(_payment(amount="0"))
        with pytest.raises(IntegrityError, match="ck_payments_amount_positive"):
            await session.commit()


async def test_duplicate_idempotency_key_rejected(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        session.add(_payment(key="dup"))
    async with session_factory() as session:
        session.add(_payment(key="dup"))
        with pytest.raises(IntegrityError, match="uq_payments_idempotency_key"):
            await session.commit()


async def test_outbox_defaults(session_factory: async_sessionmaker[AsyncSession]) -> None:
    event_id = uuid4()
    async with session_factory() as session, session.begin():
        session.add(
            OutboxEvent(
                id=event_id,
                event_type="payment.created",
                exchange="payments",
                routing_key="payments.new",
                payload={"payment_id": "x"},
            )
        )
    async with session_factory() as session:
        loaded = await session.get(OutboxEvent, event_id)
    assert loaded is not None
    assert loaded.attempts == 0
    assert loaded.published_at is None
    assert loaded.created_at.tzinfo is not None
