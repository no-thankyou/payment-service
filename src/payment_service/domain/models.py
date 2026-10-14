"""Domain entities: plain data, independent of the persistence layer."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from payment_service.domain.enums import Currency, PaymentStatus


@dataclass(frozen=True, slots=True)
class NewPayment:
    """Validated input of the "create payment" use case."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True, slots=True)
class Payment:
    id: UUID
    idempotency_key: str
    request_hash: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    webhook_attempts: int
    webhook_delivered_at: datetime | None
    created_at: datetime
    processed_at: datetime | None


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """A broker message stored in the transactional outbox until it is published."""

    id: UUID
    event_type: str
    exchange: str
    routing_key: str
    payload: dict[str, Any]
    attempts: int = 0
