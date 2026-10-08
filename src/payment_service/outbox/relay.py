"""Transactional outbox relay: publishes committed ``outbox`` rows to RabbitMQ.

Delivery is at-least-once: a crash (or failed commit) after the broker confirm but before the
commit publishes the same row again; the consumer deduplicates by payment state.
"""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from aiormq import spec
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.models import OutboxEvent

logger = logging.getLogger(__name__)

LAST_ERROR_MAX_LEN = 1000
# Bounds how long a stuck broker can keep the batch's row locks held.
PUBLISH_TIMEOUT_S = 10.0


class UnroutableMessageError(Exception):
    """RabbitMQ returned a ``mandatory`` message (``basic.return``): no queue is bound for it."""


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        batch_size: int,
        poll_interval_s: float,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s

    async def run_once(self) -> int:
        """Publish one batch of unpublished rows in ``created_at`` order; return how many were sent.

        The whole batch runs in ONE transaction that stays open while publishing. This is the
        deliberate exception to "no transaction across I/O": the ``FOR UPDATE SKIP
        LOCKED`` row locks are what stop concurrent relays from publishing the same row. Each
        publish waits for the publisher confirm, so ``published_at`` is only set for confirmed rows.

        On the first failed publish the row gets ``attempts += 1`` / ``last_error`` and the rest of
        the batch is left untouched (to preserve ordering); the transaction is still committed so
        the already published rows and the error info persist. The failed row is retried on the
        next poll.

        Unroutable messages: with ``mandatory=True`` and publisher confirms on (the factory default,
        ``on_return_raises=False``), aiormq resolves the publish with the returned message instead
        of a ``Basic.Ack`` frame -- no exception is raised. That case is detected here and treated
        as a publish failure (``UnroutableMessageError``). Broker NACKs raise
        ``aiormq.exceptions.DeliveryError`` and are handled the same way.
        """
        published = 0
        async with self._session_factory() as session, session.begin():
            stmt = (
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            rows = (await session.scalars(stmt)).all()
            for row in rows:
                try:
                    await self._publish(row)
                except Exception as exc:
                    row.attempts += 1
                    row.last_error = (str(exc) or repr(exc))[:LAST_ERROR_MAX_LEN]
                    logger.warning(
                        "outbox publish failed: id=%s event_type=%s attempts=%d error=%r",
                        row.id,
                        row.event_type,
                        row.attempts,
                        exc,
                    )
                    break
                row.published_at = datetime.now(UTC)
                published += 1
        if published:
            logger.debug("outbox relay published %d event(s)", published)
        return published

    async def _publish(self, row: OutboxEvent) -> None:
        # A str exchange is wrapped by FastStream into ``RabbitExchange(name)`` and opened with a
        # passive declare (``declare=False``), i.e. it is never (re)declared with other options.
        result = await self._broker.publish(
            row.payload,
            exchange=row.exchange,
            routing_key=row.routing_key,
            message_id=str(row.id),
            persist=True,
            mandatory=True,
            content_type="application/json",
            headers={"x-event-type": row.event_type},
            timeout=PUBLISH_TIMEOUT_S,
        )
        # None: confirms disabled (not our setup). Anything but Ack: returned (unroutable) message.
        if result is not None and not isinstance(result, spec.Basic.Ack):
            raise UnroutableMessageError(
                f"message returned by broker: exchange={row.exchange!r} "
                f"routing_key={row.routing_key!r}"
            )

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Poll until ``stop`` is set. Never crashes on errors (e.g. DB or broker down)."""
        logger.info(
            "outbox relay started (batch_size=%d, poll_interval_s=%s)",
            self._batch_size,
            self._poll_interval_s,
        )
        while not stop.is_set():
            try:
                published = await self.run_once()
            except Exception:
                logger.exception("outbox relay iteration failed")
                published = 0
            if published >= self._batch_size:
                continue  # backlog: next batch right away
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_interval_s)
        logger.info("outbox relay stopped")
