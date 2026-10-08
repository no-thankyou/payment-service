import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
import respx

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import TransientError, WebhookDeliveryError
from payment_service.services.webhook import WebhookPayload, WebhookSender

URL = "https://merchant.example.com/hooks/payments"


@pytest.fixture
async def sender() -> AsyncIterator[WebhookSender]:
    async with httpx.AsyncClient() as client:
        yield WebhookSender(client, timeout_s=1.0)


def _payload() -> WebhookPayload:
    return WebhookPayload(
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("10.5"),
        currency=Currency.USD,
        processed_at=datetime(2026, 10, 8, 12, 0, 3, tzinfo=UTC),
        metadata={"order_id": "1234", "customer_id": 42},
    )


@pytest.mark.parametrize("status_code", [200, 204])
@respx.mock
async def test_2xx_is_success(sender: WebhookSender, status_code: int) -> None:
    route = respx.post(URL).mock(return_value=httpx.Response(status_code))
    payload = _payload()

    await sender.send(URL, payload)

    assert route.call_count == 1
    request = route.calls.last.request
    assert request.headers["content-type"] == "application/json"
    assert request.headers["x-payment-id"] == str(payload.payment_id)
    assert json.loads(request.content) == {
        "event": "payment.processed",
        "payment_id": str(payload.payment_id),
        "status": "succeeded",
        "amount": "10.50",
        "currency": "USD",
        "processed_at": "2026-10-08T12:00:03Z",
        "metadata": {"order_id": "1234", "customer_id": 42},
    }


@pytest.mark.parametrize("status_code", [500, 302, 404])
@respx.mock
async def test_non_2xx_raises(sender: WebhookSender, status_code: int) -> None:
    headers = {"Location": "https://elsewhere.example.com/"} if status_code == 302 else None
    respx.post(URL).mock(return_value=httpx.Response(status_code, headers=headers))
    respx.post("https://elsewhere.example.com/").mock(return_value=httpx.Response(200))

    with pytest.raises(WebhookDeliveryError, match=str(status_code)):
        await sender.send(URL, _payload())


@pytest.mark.parametrize(
    "error",
    [httpx.ReadTimeout("timed out"), httpx.ConnectError("connection refused")],
)
@respx.mock
async def test_transport_errors_raise(sender: WebhookSender, error: httpx.HTTPError) -> None:
    respx.post(URL).mock(side_effect=error)

    with pytest.raises(WebhookDeliveryError) as exc_info:
        await sender.send(URL, _payload())
    assert isinstance(exc_info.value, TransientError)
