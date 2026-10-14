"""Transactional outbox relay: publishes committed ``outbox`` rows to RabbitMQ.

Delivery is at-least-once: a crash (or failed commit) after the broker confirm but before the
commit publishes the same row again; the consumer deduplicates by payment state.
"""

import asyncio
import contextlib
import logging

from aiormq import spec
from faststream.rabbit import RabbitBroker

from payment_service.domain.models import OutboxMessage
from payment_service.domain.repositories import UnitOfWorkFactory

logger = logging.getLogger(__name__)

LAST_ERROR_MAX_LEN = 1000
# Bounds how long a stuck broker can keep the batch's row locks held.
PUBLISH_TIMEOUT_S = 10.0


class UnroutableMessageError(Exception):
    """RabbitMQ returned a ``mandatory`` message (``basic.return``): no queue is bound for it."""


class OutboxRelay:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        broker: RabbitBroker,
        batch_size: int,
        poll_interval_s: float,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        self._uow_factory = uow_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s

    async def run_once(self) -> int:
        """Publish one batch of unpublished rows in ``created_at`` order; return how many were sent.

        The whole batch runs in ONE unit of work that stays open while publishing. This is the
        deliberate exception to "no transaction across I/O": the row locks taken by
        ``lock_unpublished`` (``FOR UPDATE SKIP LOCKED``) are what stop concurrent relays from
        publishing the same message. Each publish waits for the publisher confirm, so a message is
        only marked published once confirmed.

        On the first failed publish the message gets ``attempts += 1`` / ``last_error`` and the
        rest of the batch is left untouched (to preserve ordering); the unit of work is still
        committed so the already published messages and the error info persist. The failed one is
        retried on the next poll.

        Unroutable messages: with ``mandatory=True`` and publisher confirms on (the factory default,
        ``on_return_raises=False``), aiormq resolves the publish with the returned message instead
        of a ``Basic.Ack`` frame -- no exception is raised. That case is detected here and treated
        as a publish failure (``UnroutableMessageError``). Broker NACKs raise
        ``aiormq.exceptions.DeliveryError`` and are handled the same way.
        """
        published = 0
        async with self._uow_factory() as uow:
            messages = await uow.outbox.lock_unpublished(self._batch_size)
            for message in messages:
                try:
                    await self._publish(message)
                except Exception as exc:
                    error = (str(exc) or repr(exc))[:LAST_ERROR_MAX_LEN]
                    attempts = await uow.outbox.record_failure(message.id, error)
                    logger.warning(
                        "outbox publish failed: id=%s event_type=%s attempts=%d error=%r",
                        message.id,
                        message.event_type,
                        attempts,
                        exc,
                    )
                    break
                await uow.outbox.mark_published(message.id)
                published += 1
            await uow.commit()
        if published:
            logger.debug("outbox relay published %d event(s)", published)
        return published

    async def _publish(self, message: OutboxMessage) -> None:
        # A str exchange is wrapped by FastStream into ``RabbitExchange(name)`` and opened with a
        # passive declare (``declare=False``), i.e. it is never (re)declared with other options.
        result = await self._broker.publish(
            message.payload,
            exchange=message.exchange,
            routing_key=message.routing_key,
            message_id=str(message.id),
            persist=True,
            mandatory=True,
            content_type="application/json",
            headers={"x-event-type": message.event_type},
            timeout=PUBLISH_TIMEOUT_S,
        )
        # None: confirms disabled (not our setup). Anything but Ack: returned (unroutable) message.
        if result is not None and not isinstance(result, spec.Basic.Ack):
            raise UnroutableMessageError(
                f"message returned by broker: exchange={message.exchange!r} "
                f"routing_key={message.routing_key!r}"
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
