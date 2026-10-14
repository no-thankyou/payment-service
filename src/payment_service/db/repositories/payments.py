"""SQLAlchemy implementation of ``PaymentRepository``."""

from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db import models
from payment_service.domain.enums import PaymentStatus
from payment_service.domain.models import NewPayment, Payment


def _to_domain(row: models.Payment) -> Payment:
    return Payment(
        id=row.id,
        idempotency_key=row.idempotency_key,
        request_hash=row.request_hash,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        metadata=row.metadata_,
        status=row.status,
        webhook_url=row.webhook_url,
        webhook_attempts=row.webhook_attempts,
        webhook_delivered_at=row.webhook_delivered_at,
        created_at=row.created_at,
        processed_at=row.processed_at,
    )


class SqlAlchemyPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_if_absent(
        self, payment_id: UUID, idempotency_key: str, request_hash: str, data: NewPayment
    ) -> Payment | None:
        # Under READ COMMITTED a concurrent INSERT ... ON CONFLICT DO NOTHING with the same key
        # waits for the other transaction and then skips; the unique index resolves the race.
        stmt = (
            insert(models.Payment)
            .values(
                id=payment_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                status=PaymentStatus.PENDING,
                webhook_url=data.webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[models.Payment.idempotency_key])
            .returning(models.Payment)
        )
        row = (await self._session.scalars(stmt)).one_or_none()
        return None if row is None else _to_domain(row)

    async def get(self, payment_id: UUID) -> Payment | None:
        row = await self._session.get(models.Payment, payment_id)
        return None if row is None else _to_domain(row)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(models.Payment).where(models.Payment.idempotency_key == idempotency_key)
        row = (await self._session.scalars(stmt)).one_or_none()
        return None if row is None else _to_domain(row)

    async def set_status_if_pending(self, payment_id: UUID, status: PaymentStatus) -> bool:
        stmt = (
            update(models.Payment)
            .where(models.Payment.id == payment_id, models.Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(models.Payment.id)
        )
        return (await self._session.scalars(stmt)).one_or_none() is not None

    async def increment_webhook_attempts(self, payment_id: UUID) -> None:
        stmt = (
            update(models.Payment)
            .where(models.Payment.id == payment_id)
            .values(webhook_attempts=models.Payment.webhook_attempts + 1)
        )
        await self._session.execute(stmt)

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        stmt = (
            update(models.Payment)
            .where(models.Payment.id == payment_id, models.Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=func.now())
        )
        await self._session.execute(stmt)
