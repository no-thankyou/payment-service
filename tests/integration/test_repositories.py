"""SQLAlchemy repositories and unit of work against a real PostgreSQL."""

import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.uow import SqlAlchemyUnitOfWork
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.models import NewPayment, OutboxMessage

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_db")]

SessionFactory = async_sessionmaker[AsyncSession]

DATA = NewPayment(
    amount=Decimal("10.50"),
    currency=Currency.EUR,
    description="repo test",
    metadata={"k": "v"},
    webhook_url="https://merchant.example.com/hooks",
)


def _message() -> OutboxMessage:
    return OutboxMessage(
        id=uuid4(),
        event_type="payment.created",
        exchange="payments",
        routing_key="payments.new",
        payload={"x": 1},
    )


async def test_add_if_absent_returns_none_on_key_conflict(session_factory: SessionFactory) -> None:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        created = await uow.payments.add_if_absent(uuid4(), "key-1", "h" * 64, DATA)
        await uow.commit()
    assert created is not None
    assert created.status is PaymentStatus.PENDING
    assert created.webhook_attempts == 0
    assert created.created_at.tzinfo is not None

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        assert await uow.payments.add_if_absent(uuid4(), "key-1", "h" * 64, DATA) is None
        assert await uow.payments.get_by_idempotency_key("key-1") == created
        assert await uow.payments.get(created.id) == created
        assert await uow.payments.get_by_idempotency_key("other") is None


async def test_uncommitted_work_is_rolled_back(session_factory: SessionFactory) -> None:
    payment_id = uuid4()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.payments.add_if_absent(payment_id, "key-1", "h" * 64, DATA)
        await uow.outbox.add(_message())

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        assert await uow.payments.get(payment_id) is None
        assert await uow.outbox.lock_unpublished(10) == []


async def test_payment_state_updates(session_factory: SessionFactory) -> None:
    payment_id = uuid4()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.payments.add_if_absent(payment_id, "key-1", "h" * 64, DATA)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        assert await uow.payments.set_status_if_pending(payment_id, PaymentStatus.SUCCEEDED)
        assert not await uow.payments.set_status_if_pending(payment_id, PaymentStatus.FAILED)
        await uow.payments.increment_webhook_attempts(payment_id)
        await uow.payments.increment_webhook_attempts(payment_id)
        await uow.payments.mark_webhook_delivered(payment_id)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        payment = await uow.payments.get(payment_id)
        assert payment is not None
        delivered_at = payment.webhook_delivered_at
        await uow.payments.mark_webhook_delivered(payment_id)
        await uow.commit()
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_attempts == 2
    assert delivered_at is not None

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        again = await uow.payments.get(payment_id)
    assert again is not None
    assert again.webhook_delivered_at == delivered_at


async def test_outbox_lifecycle(session_factory: SessionFactory) -> None:
    first, second = _message(), _message()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.outbox.add(first)
        await uow.commit()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.outbox.add(second)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        assert await uow.outbox.lock_unpublished(10) == [first, second]
        await uow.outbox.mark_published(first.id)
        assert await uow.outbox.record_failure(second.id, "boom") == 1
        assert await uow.outbox.record_failure(second.id, "boom") == 2
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        (pending,) = await uow.outbox.lock_unpublished(10)
    assert pending.id == second.id
    assert pending.attempts == 2


async def test_locked_outbox_messages_are_skipped(session_factory: SessionFactory) -> None:
    message = _message()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.outbox.add(message)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as holder:
        assert await holder.outbox.lock_unpublished(10) == [message]
        async with SqlAlchemyUnitOfWork(session_factory) as other:
            assert await asyncio.wait_for(other.outbox.lock_unpublished(10), timeout=5) == []
