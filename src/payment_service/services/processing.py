"""Consumer use case: charge a payment and deliver its webhook.

Broker-agnostic and idempotent on redelivery. Every DB access is a short, separate
transaction; none is held across the gateway call or the webhook HTTP request.
"""

import logging
from uuid import UUID

from payment_service.domain.enums import PaymentStatus
from payment_service.domain.errors import PaymentNotFoundError, WebhookDeliveryError
from payment_service.domain.models import Payment
from payment_service.domain.repositories import UnitOfWorkFactory
from payment_service.services.gateway import PaymentGateway
from payment_service.services.webhook import WebhookPayload, WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhook: WebhookSender,
    ) -> None:
        self._uow_factory = uow_factory
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
            metadata=payment.metadata,
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
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _apply_status(self, payment_id: UUID, status: PaymentStatus) -> bool:
        """Conditional terminal update; False if another delivery already finished."""
        async with self._uow_factory() as uow:
            applied = await uow.payments.set_status_if_pending(payment_id, status)
            await uow.commit()
        return applied

    async def _increment_webhook_attempts(self, payment_id: UUID) -> None:
        async with self._uow_factory() as uow:
            await uow.payments.increment_webhook_attempts(payment_id)
            await uow.commit()

    async def _mark_webhook_delivered(self, payment_id: UUID) -> None:
        async with self._uow_factory() as uow:
            await uow.payments.mark_webhook_delivered(payment_id)
            await uow.commit()
