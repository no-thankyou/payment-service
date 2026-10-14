"""``PaymentProcessor`` flow against in-memory repositories (no database)."""

from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest
import respx

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import PaymentNotFoundError, WebhookDeliveryError
from payment_service.domain.models import Payment
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhook import WebhookSender
from tests.unit.fakes import FakeUnitOfWork, InMemoryStore

WEBHOOK_URL = "https://merchant.example.com/hooks/payments"


class FakeGateway:
    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self._status = status
        self.calls: list[UUID] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> PaymentStatus:
        self.calls.append(payment_id)
        return self._status


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
async def sender() -> AsyncIterator[WebhookSender]:
    async with httpx.AsyncClient() as client:
        yield WebhookSender(client, timeout_s=1.0)


@pytest.fixture
def mock_http() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as router:
        yield router


def _processor(
    store: InMemoryStore, gateway: FakeGateway, sender: WebhookSender
) -> PaymentProcessor:
    return PaymentProcessor(lambda: FakeUnitOfWork(store), gateway, sender)


def _seed(store: InMemoryStore, **overrides: object) -> UUID:
    payment = Payment(
        id=uuid4(),
        idempotency_key="key-1",
        request_hash="0" * 64,
        amount=Decimal("10.50"),
        currency=Currency.USD,
        description="test payment",
        metadata={"order_id": "1234"},
        status=PaymentStatus.PENDING,
        webhook_url=WEBHOOK_URL,
        webhook_attempts=0,
        webhook_delivered_at=None,
        created_at=datetime.now(UTC),
        processed_at=None,
    )
    payment = replace(payment, **overrides)  # type: ignore[arg-type]
    store.payments[payment.id] = payment
    return payment.id


async def test_pending_payment_is_charged_and_webhook_delivered(
    store: InMemoryStore, sender: WebhookSender, mock_http: respx.MockRouter
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = _seed(store)
    gateway = FakeGateway(PaymentStatus.FAILED)

    await _processor(store, gateway, sender).process(payment_id)

    payment = store.payments[payment_id]
    assert gateway.calls == [payment_id]
    assert payment.status is PaymentStatus.FAILED
    assert payment.processed_at is not None
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at is not None
    assert route.call_count == 1


async def test_failed_webhook_counts_the_attempt_and_is_not_marked_delivered(
    store: InMemoryStore, sender: WebhookSender, mock_http: respx.MockRouter
) -> None:
    mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(503))
    payment_id = _seed(store)

    with pytest.raises(WebhookDeliveryError):
        await _processor(store, FakeGateway(), sender).process(payment_id)

    payment = store.payments[payment_id]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at is None


async def test_processed_payment_is_not_charged_again(
    store: InMemoryStore, sender: WebhookSender, mock_http: respx.MockRouter
) -> None:
    mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = _seed(store, status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))
    gateway = FakeGateway(PaymentStatus.FAILED)

    await _processor(store, gateway, sender).process(payment_id)

    assert gateway.calls == []
    assert store.payments[payment_id].status is PaymentStatus.SUCCEEDED
    assert store.payments[payment_id].webhook_delivered_at is not None


async def test_delivered_payment_is_a_noop(
    store: InMemoryStore, sender: WebhookSender, mock_http: respx.MockRouter
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = _seed(
        store,
        status=PaymentStatus.SUCCEEDED,
        webhook_attempts=1,
        webhook_delivered_at=datetime.now(UTC),
    )

    await _processor(store, FakeGateway(), sender).process(payment_id)

    assert route.call_count == 0
    assert store.payments[payment_id].webhook_attempts == 1


async def test_unknown_payment_raises_not_found(
    store: InMemoryStore, sender: WebhookSender
) -> None:
    gateway = FakeGateway()
    with pytest.raises(PaymentNotFoundError):
        await _processor(store, gateway, sender).process(uuid4())
    assert gateway.calls == []
