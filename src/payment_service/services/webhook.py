"""Webhook delivery."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, Field, PlainSerializer

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import WebhookDeliveryError

logger = logging.getLogger(__name__)

WEBHOOK_EVENT: Literal["payment.processed"] = "payment.processed"

MoneyStr = Annotated[Decimal, PlainSerializer(lambda v: f"{v:.2f}", return_type=str)]


class WebhookPayload(BaseModel):
    event: Literal["payment.processed"] = WEBHOOK_EVENT
    payment_id: UUID
    status: PaymentStatus
    amount: MoneyStr
    currency: Currency
    processed_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class WebhookSender:
    """POSTs the payload; any non-2xx / timeout / network error -> ``WebhookDeliveryError``.

    The ``httpx.AsyncClient`` is owned by the caller (created and closed outside).
    """

    def __init__(self, client: httpx.AsyncClient, timeout_s: float) -> None:
        self._client = client
        self._timeout_s = timeout_s

    async def send(self, url: str, payload: WebhookPayload) -> None:
        payment_id = str(payload.payment_id)
        try:
            response = await self._client.post(
                url,
                json=payload.model_dump(mode="json"),
                headers={"Content-Type": "application/json", "X-Payment-Id": payment_id},
                timeout=self._timeout_s,
                follow_redirects=False,
            )
        except httpx.TimeoutException as exc:
            raise WebhookDeliveryError(
                f"webhook for payment {payment_id} timed out: {type(exc).__name__}"
            ) from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(
                f"webhook for payment {payment_id} failed: {type(exc).__name__}: {exc}"
            ) from exc

        if not response.is_success:
            raise WebhookDeliveryError(
                f"webhook for payment {payment_id} got HTTP {response.status_code}"
            )
        logger.info("webhook delivered payment_id=%s status=%s", payment_id, response.status_code)
