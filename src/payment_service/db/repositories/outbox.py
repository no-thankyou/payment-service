"""SQLAlchemy implementation of ``OutboxRepository``."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db import models
from payment_service.domain.models import OutboxMessage


def _to_domain(row: models.OutboxEvent) -> OutboxMessage:
    return OutboxMessage(
        id=row.id,
        event_type=row.event_type,
        exchange=row.exchange,
        routing_key=row.routing_key,
        payload=row.payload,
        attempts=row.attempts,
    )


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: OutboxMessage) -> None:
        self._session.add(
            models.OutboxEvent(
                id=message.id,
                event_type=message.event_type,
                exchange=message.exchange,
                routing_key=message.routing_key,
                payload=message.payload,
                attempts=message.attempts,
            )
        )

    async def lock_unpublished(self, limit: int) -> list[OutboxMessage]:
        stmt = (
            select(models.OutboxEvent)
            .where(models.OutboxEvent.published_at.is_(None))
            .order_by(models.OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [_to_domain(row) for row in await self._session.scalars(stmt)]

    async def mark_published(self, message_id: UUID) -> None:
        stmt = (
            update(models.OutboxEvent)
            .where(models.OutboxEvent.id == message_id)
            .values(published_at=datetime.now(UTC))
        )
        await self._session.execute(stmt)

    async def record_failure(self, message_id: UUID, error: str) -> int:
        stmt = (
            update(models.OutboxEvent)
            .where(models.OutboxEvent.id == message_id)
            .values(attempts=models.OutboxEvent.attempts + 1, last_error=error)
            .returning(models.OutboxEvent.attempts)
        )
        return (await self._session.scalars(stmt)).one()
