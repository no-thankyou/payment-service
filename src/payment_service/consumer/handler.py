"""Message outcome routing for ``payments.new``.

- success → return (the subscriber's ack policy acks);
- ``PermanentError`` → re-raise → ``reject(requeue=False)`` → ``payments.dlq``;
- any other exception, ``attempt < max`` → publish a confirmed copy to ``payments.retry`` with
  ``x-attempt + 1`` and return (ack); ``attempt >= max`` → re-raise → DLQ;
- the retry publish itself fails (error, timeout or returned as unroutable) → re-raise →
  ``reject(requeue=False)`` → DLQ. Requeueing instead would redeliver immediately (hot loop, extra
  webhooks) and break the "at most ``RETRY_MAX_ATTEMPTS`` deliveries" contract.
"""

import logging
from typing import Any

from aiormq import spec
from faststream.rabbit import RabbitBroker, RabbitMessage

from payment_service.broker.topology import (
    RETRY_EXCHANGE,
    retry_delay_for_attempt,
    retry_routing_key,
)
from payment_service.core.config import Settings
from payment_service.domain.errors import PermanentError
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"
LAST_ERROR_MAX_LEN = 200
RETRY_PUBLISH_TIMEOUT_S = 10.0
# Broker-managed dead-lettering headers; not copied onto the retry message we publish ourselves.
_BROKER_HEADER_PREFIXES = ("x-death", "x-first-death-", "x-last-death-")


class RetryPublishError(Exception):
    """The retry copy was not confirmed by the broker (e.g. returned as unroutable)."""


def attempt_of(msg: RabbitMessage) -> int:
    """Delivery number from the ``x-attempt`` header (absent or invalid ⇒ 1)."""
    raw = msg.headers.get(ATTEMPT_HEADER, 1)
    try:
        attempt = int(raw)
    except (TypeError, ValueError):
        return 1
    return max(attempt, 1)


class PaymentEventHandler:
    def __init__(
        self, settings: Settings, broker: RabbitBroker, processor: PaymentProcessor
    ) -> None:
        self._settings = settings
        self._broker = broker
        self._processor = processor

    async def handle(self, event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
        attempt = attempt_of(msg)
        try:
            await self._processor.process(event.payment_id)
        except PermanentError:
            logger.exception(
                "permanent error, dead-lettering payment_id=%s event_id=%s attempt=%d",
                event.payment_id,
                event.event_id,
                attempt,
            )
            raise
        except Exception as exc:
            if attempt >= self._settings.retry_max_attempts:
                logger.exception(
                    "attempts exhausted, dead-lettering payment_id=%s event_id=%s attempt=%d",
                    event.payment_id,
                    event.event_id,
                    attempt,
                )
                raise
            await self._schedule_retry(event, msg, attempt, exc)
            return
        logger.info(
            "event processed payment_id=%s event_id=%s attempt=%d",
            event.payment_id,
            event.event_id,
            attempt,
        )

    async def _schedule_retry(
        self, event: PaymentCreatedEvent, msg: RabbitMessage, attempt: int, exc: Exception
    ) -> None:
        delay_ms = retry_delay_for_attempt(self._settings, attempt)
        headers: dict[str, Any] = {
            k: v for k, v in msg.headers.items() if not k.startswith(_BROKER_HEADER_PREFIXES)
        }
        headers[ATTEMPT_HEADER] = attempt + 1
        headers[LAST_ERROR_HEADER] = (str(exc) or repr(exc))[:LAST_ERROR_MAX_LEN]
        try:
            result = await self._broker.publish(
                msg.body,
                exchange=RETRY_EXCHANGE,
                routing_key=retry_routing_key(delay_ms),
                headers=headers,
                message_id=msg.message_id,
                correlation_id=msg.correlation_id,
                content_type="application/json",
                persist=True,
                mandatory=True,
                timeout=RETRY_PUBLISH_TIMEOUT_S,
            )
            # Same contract as the outbox relay: anything but Ack is a returned message.
            if result is not None and not isinstance(result, spec.Basic.Ack):
                raise RetryPublishError(f"retry message returned by broker: delay={delay_ms}ms")
        except Exception:
            logger.exception(
                "retry publish failed, dead-lettering payment_id=%s event_id=%s attempt=%d",
                event.payment_id,
                event.event_id,
                attempt,
            )
            # REJECT_ON_ERROR rejects without requeue → payments.dlq (no hot redelivery loop).
            raise
        logger.warning(
            "processing failed, retry %d/%d in %dms payment_id=%s event_id=%s error=%r",
            attempt + 1,
            self._settings.retry_max_attempts,
            delay_ms,
            event.payment_id,
            event.event_id,
            exc,
        )
