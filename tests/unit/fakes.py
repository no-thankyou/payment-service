"""In-memory implementations of the data-access ports for service unit tests.

``InMemoryStore`` is the "database"; each ``FakeUnitOfWork`` works on a copy of it and writes the
copy back only on ``commit()``, so rollback-on-exit behaves like a real transaction.
"""

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import TracebackType
from typing import Self
from uuid import UUID

from payment_service.domain.enums import PaymentStatus
from payment_service.domain.models import NewPayment, OutboxMessage, Payment


@dataclass
class InMemoryStore:
    payments: dict[UUID, Payment] = field(default_factory=dict)
    outbox: dict[UUID, OutboxMessage] = field(default_factory=dict)
    published: set[UUID] = field(default_factory=set)
    errors: dict[UUID, str] = field(default_factory=dict)
    commits: int = 0


class InMemoryPaymentRepository:
    def __init__(self, payments: dict[UUID, Payment]) -> None:
        self._payments = payments

    async def add_if_absent(
        self, payment_id: UUID, idempotency_key: str, request_hash: str, data: NewPayment
    ) -> Payment | None:
        if await self.get_by_idempotency_key(idempotency_key) is not None:
            return None
        payment = Payment(
            id=payment_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata=data.metadata,
            status=PaymentStatus.PENDING,
            webhook_url=data.webhook_url,
            webhook_attempts=0,
            webhook_delivered_at=None,
            created_at=datetime.now(UTC),
            processed_at=None,
        )
        self._payments[payment_id] = payment
        return payment

    async def get(self, payment_id: UUID) -> Payment | None:
        return self._payments.get(payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return next(
            (p for p in self._payments.values() if p.idempotency_key == idempotency_key), None
        )

    async def set_status_if_pending(self, payment_id: UUID, status: PaymentStatus) -> bool:
        payment = self._payments.get(payment_id)
        if payment is None or payment.status != PaymentStatus.PENDING:
            return False
        self._payments[payment_id] = replace(payment, status=status, processed_at=datetime.now(UTC))
        return True

    async def increment_webhook_attempts(self, payment_id: UUID) -> None:
        if (payment := self._payments.get(payment_id)) is not None:
            self._payments[payment_id] = replace(
                payment, webhook_attempts=payment.webhook_attempts + 1
            )

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        payment = self._payments.get(payment_id)
        if payment is not None and payment.webhook_delivered_at is None:
            self._payments[payment_id] = replace(payment, webhook_delivered_at=datetime.now(UTC))


class InMemoryOutboxRepository:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    async def add(self, message: OutboxMessage) -> None:
        self._store.outbox[message.id] = message

    async def lock_unpublished(self, limit: int) -> list[OutboxMessage]:
        pending = [m for m in self._store.outbox.values() if m.id not in self._store.published]
        return pending[:limit]

    async def mark_published(self, message_id: UUID) -> None:
        self._store.published.add(message_id)

    async def record_failure(self, message_id: UUID, error: str) -> int:
        message = self._store.outbox[message_id]
        self._store.outbox[message_id] = replace(message, attempts=message.attempts + 1)
        self._store.errors[message_id] = error
        return message.attempts + 1


class FakeUnitOfWork:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store
        self._work = InMemoryStore(
            payments=dict(store.payments),
            outbox=dict(store.outbox),
            published=set(store.published),
            errors=dict(store.errors),
        )
        self.payments = InMemoryPaymentRepository(self._work.payments)
        self.outbox = InMemoryOutboxRepository(self._work)

    async def commit(self) -> None:
        self._store.payments = dict(self._work.payments)
        self._store.outbox = dict(self._work.outbox)
        self._store.published = set(self._work.published)
        self._store.errors = dict(self._work.errors)
        self._store.commits += 1

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None
