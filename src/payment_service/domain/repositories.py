"""Data-access ports used by the services. Implementations live in ``payment_service.db``.

Every repository works inside the transaction of the ``UnitOfWork`` that owns it; nothing is
persisted until ``UnitOfWork.commit()``.
"""

from collections.abc import Callable
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from payment_service.domain.enums import PaymentStatus
from payment_service.domain.models import NewPayment, OutboxMessage, Payment


class PaymentRepository(Protocol):
    async def add_if_absent(
        self, payment_id: UUID, idempotency_key: str, request_hash: str, data: NewPayment
    ) -> Payment | None:
        """Insert a ``PENDING`` payment; ``None`` if ``idempotency_key`` is already taken.

        A concurrent insert with the same key blocks until the other transaction finishes.
        """
        ...

    async def get(self, payment_id: UUID) -> Payment | None: ...

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None: ...

    async def set_status_if_pending(self, payment_id: UUID, status: PaymentStatus) -> bool:
        """Set the terminal status and ``processed_at``; ``False`` if it is no longer pending."""
        ...

    async def increment_webhook_attempts(self, payment_id: UUID) -> None: ...

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        """Set ``webhook_delivered_at`` unless it is already set."""
        ...


class OutboxRepository(Protocol):
    async def add(self, message: OutboxMessage) -> None: ...

    async def lock_unpublished(self, limit: int) -> list[OutboxMessage]:
        """Oldest unpublished messages, locked until the end of the transaction.

        Messages locked by another transaction are skipped, not waited for.
        """
        ...

    async def mark_published(self, message_id: UUID) -> None: ...

    async def record_failure(self, message_id: UUID, error: str) -> int:
        """Increment ``attempts``, store ``error``; return the new number of attempts."""
        ...


class UnitOfWork(Protocol):
    """One database transaction. Rolled back on exit unless ``commit()`` was called."""

    @property
    def payments(self) -> PaymentRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    async def commit(self) -> None: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]
