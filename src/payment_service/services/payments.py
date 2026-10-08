"""Payment creation (idempotency + transactional outbox) and lookup. Framework-agnostic."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker.topology import NEW_ROUTING_KEY, PAYMENTS_EXCHANGE_NAME
from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError
from payment_service.domain.events import PAYMENT_CREATED, PaymentCreatedEvent

_CENTS = Decimal("0.01")


@dataclass(frozen=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool


def compute_request_hash(p: NewPayment) -> str:
    """sha256 of the canonical request JSON. Must stay stable."""
    canonical = {
        "amount": str(p.amount.quantize(_CENTS)),
        "currency": p.currency.value,
        "description": p.description,
        "metadata": p.metadata,
        "webhook_url": str(p.webhook_url),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def create_payment(
    session_factory: async_sessionmaker[AsyncSession],
    idempotency_key: str,
    data: NewPayment,
) -> CreatePaymentResult:
    """Create a payment + outbox row atomically, or replay the existing one for the same key.

    Races are resolved by the unique index on ``idempotency_key``: under READ COMMITTED a
    concurrent ``INSERT ... ON CONFLICT DO NOTHING`` waits for the other transaction and then
    skips; the following SELECT (a new statement) sees the committed row.
    """
    request_hash = compute_request_hash(data)
    async with session_factory() as session, session.begin():
        stmt = (
            insert(Payment)
            .values(
                id=uuid4(),
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                status=PaymentStatus.PENDING,
                webhook_url=data.webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        inserted_id = (await session.execute(stmt)).scalar_one_or_none()

        if inserted_id is not None:
            event = PaymentCreatedEvent(
                event_id=uuid4(), payment_id=inserted_id, occurred_at=datetime.now(UTC)
            )
            session.add(
                OutboxEvent(
                    id=event.event_id,
                    event_type=PAYMENT_CREATED,
                    exchange=PAYMENTS_EXCHANGE_NAME,
                    routing_key=NEW_ROUTING_KEY,
                    payload=event.model_dump(mode="json"),
                )
            )
            payment = await session.get_one(Payment, inserted_id)
            return CreatePaymentResult(payment=payment, replayed=False)

        # Payments are never deleted, so the conflicting row is guaranteed to exist.
        existing = (
            await session.scalars(select(Payment).where(Payment.idempotency_key == idempotency_key))
        ).one()
        if existing.request_hash != request_hash:
            raise IdempotencyKeyReusedError(
                f"Idempotency-Key {idempotency_key!r} was used with a different request body"
            )
        return CreatePaymentResult(payment=existing, replayed=True)


async def get_payment(
    session_factory: async_sessionmaker[AsyncSession], payment_id: UUID
) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFoundError(payment_id)
    return payment
