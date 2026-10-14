"""Payment creation (idempotency + transactional outbox) and lookup. Framework-agnostic."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from payment_service.broker.topology import NEW_ROUTING_KEY, PAYMENTS_EXCHANGE_NAME
from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError
from payment_service.domain.events import PAYMENT_CREATED, PaymentCreatedEvent
from payment_service.domain.models import NewPayment, OutboxMessage, Payment
from payment_service.domain.repositories import UnitOfWorkFactory

_CENTS = Decimal("0.01")


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


class PaymentService:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def create(self, idempotency_key: str, data: NewPayment) -> CreatePaymentResult:
        """Create a payment + outbox message atomically, or replay the existing one for the key.

        Concurrent requests with the same key are serialised by the repository (unique key); the
        loser of the race then reads the winner's committed payment.
        """
        request_hash = compute_request_hash(data)
        async with self._uow_factory() as uow:
            payment = await uow.payments.add_if_absent(uuid4(), idempotency_key, request_hash, data)
            if payment is not None:
                event = PaymentCreatedEvent(
                    event_id=uuid4(), payment_id=payment.id, occurred_at=datetime.now(UTC)
                )
                await uow.outbox.add(
                    OutboxMessage(
                        id=event.event_id,
                        event_type=PAYMENT_CREATED,
                        exchange=PAYMENTS_EXCHANGE_NAME,
                        routing_key=NEW_ROUTING_KEY,
                        payload=event.model_dump(mode="json"),
                    )
                )
                await uow.commit()
                return CreatePaymentResult(payment=payment, replayed=False)

            existing = await uow.payments.get_by_idempotency_key(idempotency_key)

        if existing is None:  # pragma: no cover - payments are never deleted
            raise RuntimeError(f"payment for Idempotency-Key {idempotency_key!r} disappeared")
        if existing.request_hash != request_hash:
            raise IdempotencyKeyReusedError(
                f"Idempotency-Key {idempotency_key!r} was used with a different request body"
            )
        return CreatePaymentResult(payment=existing, replayed=True)

    async def get(self, payment_id: UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
