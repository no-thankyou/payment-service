"""Consumer use case: charge a payment and deliver its webhook.

Broker-agnostic and idempotent on redelivery. Every DB access is a short, separate
transaction; none is held across the gateway call or the webhook HTTP request.
"""

import logging
from uuid import UUID

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus
from payment_service.domain.errors import PaymentNotFoundError, WebhookDeliveryError
from payment_service.services.gateway import PaymentGateway
from payment_service.services.webhook import WebhookPayload, WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, payment_id: UUID) -> None:
        payment = await self._load(payment_id)

        if payment.status == PaymentStatus.PENDING:
            status = await self._gateway.charge(payment.id, payment.amount, payment.currency)
            if await self._apply_status(payment_id, status):
                logger.info("payment processed payment_id=%s status=%s", payment_id, status.value)
            else:
                logger.info("payment already processed concurrently payment_id=%s", payment_id)
            payment = await self._load(payment_id)

        if payment.webhook_delivered_at is not None:
            logger.info("webhook already delivered, skipping payment_id=%s", payment_id)
            return

        payload = WebhookPayload(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
            metadata=payment.metadata_,
        )
        # Counted before the POST (own short tx) so that an attempt is recorded even if the
        # process dies mid-request; delivery is marked in a separate tx after a 2xx.
        await self._increment_webhook_attempts(payment_id)
        try:
            await self._webhook.send(payment.webhook_url, payload)
        except WebhookDeliveryError:
            logger.warning("webhook delivery failed payment_id=%s", payment_id)
            raise
        await self._mark_webhook_delivered(payment_id)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _apply_status(self, payment_id: UUID, status: PaymentStatus) -> bool:
        """Conditional terminal update; False if another delivery already finished."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
        )
        async with self._session_factory.begin() as session:
            result = await session.execute(stmt)
        return bool(result.rowcount)  # type: ignore[attr-defined]

    async def _increment_webhook_attempts(self, payment_id: UUID) -> None:
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )
        async with self._session_factory.begin() as session:
            await session.execute(stmt)

    async def _mark_webhook_delivered(self, payment_id: UUID) -> None:
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=func.now())
        )
        async with self._session_factory.begin() as session:
            await session.execute(stmt)
