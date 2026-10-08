import asyncio
import json
import random
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.models import Payment
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import PaymentNotFoundError, WebhookDeliveryError
from payment_service.services.gateway import EmulatedGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhook import WebhookSender

pytestmark = pytest.mark.integration

WEBHOOK_URL = "https://merchant.example.com/hooks/payments"

SessionFactory = async_sessionmaker[AsyncSession]


class FakeGateway:
    """Returns the configured statuses in order (last one repeats) and records calls."""

    def __init__(self, *statuses: PaymentStatus) -> None:
        self._statuses = list(statuses) or [PaymentStatus.SUCCEEDED]
        self.calls: list[UUID] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> PaymentStatus:
        idx = min(len(self.calls), len(self._statuses) - 1)
        self.calls.append(payment_id)
        await asyncio.sleep(0)
        return self._statuses[idx]


@pytest.fixture
async def sender() -> AsyncIterator[WebhookSender]:
    async with httpx.AsyncClient() as client:
        yield WebhookSender(client, timeout_s=1.0)


@pytest.fixture
def mock_http() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as router:
        yield router


async def _insert_payment(
    session_factory: SessionFactory,
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    delivered: bool = False,
) -> UUID:
    payment_id = uuid4()
    terminal = status != PaymentStatus.PENDING
    async with session_factory.begin() as session:
        session.add(
            Payment(
                id=payment_id,
                idempotency_key=f"key-{payment_id}",
                request_hash="0" * 64,
                amount=Decimal("10.50"),
                currency=Currency.USD,
                description="test payment",
                metadata_={"order_id": "1234"},
                status=status,
                webhook_url=WEBHOOK_URL,
                processed_at=datetime.now(UTC) if terminal else None,
                webhook_attempts=1 if delivered else 0,
                webhook_delivered_at=datetime.now(UTC) if delivered else None,
            )
        )
    return payment_id


async def _get(session_factory: SessionFactory, payment_id: UUID) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


def _body(route: respx.Route, call: int = -1) -> dict[str, object]:
    body: dict[str, object] = json.loads(route.calls[call].request.content)
    return body


async def test_pending_succeeded_and_delivered(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _insert_payment(session_factory)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)

    await PaymentProcessor(session_factory, gateway, sender).process(payment_id)

    payment = await _get(session_factory, payment_id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at is not None
    assert gateway.calls == [payment_id]
    assert route.call_count == 1
    body = _body(route)
    assert body["payment_id"] == str(payment_id)
    assert body["status"] == "succeeded"
    assert body["amount"] == "10.50"
    assert body["metadata"] == {"order_id": "1234"}
    assert body["processed_at"] is not None


async def test_gateway_decline_is_delivered_as_failed(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _insert_payment(session_factory)

    await PaymentProcessor(session_factory, FakeGateway(PaymentStatus.FAILED), sender).process(
        payment_id
    )

    payment = await _get(session_factory, payment_id)
    assert payment.status == PaymentStatus.FAILED
    assert payment.webhook_delivered_at is not None
    assert _body(route)["status"] == "failed"


async def test_webhook_failure_then_redelivery(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(side_effect=[httpx.Response(500), httpx.Response(200)])
    payment_id = await _insert_payment(session_factory)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    processor = PaymentProcessor(session_factory, gateway, sender)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment_id)

    payment = await _get(session_factory, payment_id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at is None

    await processor.process(payment_id)

    payment = await _get(session_factory, payment_id)
    assert gateway.calls == [payment_id]
    assert route.call_count == 2
    assert payment.webhook_attempts == 2
    assert payment.webhook_delivered_at is not None
    assert _body(route)["status"] == "succeeded"


async def test_already_delivered_is_noop(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _insert_payment(
        session_factory, status=PaymentStatus.SUCCEEDED, delivered=True
    )
    gateway = FakeGateway()

    await PaymentProcessor(session_factory, gateway, sender).process(payment_id)

    payment = await _get(session_factory, payment_id)
    assert gateway.calls == []
    assert route.call_count == 0
    assert payment.webhook_attempts == 1


async def test_unknown_payment_raises_not_found(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
) -> None:
    gateway = FakeGateway()
    with pytest.raises(PaymentNotFoundError):
        await PaymentProcessor(session_factory, gateway, sender).process(uuid4())
    assert gateway.calls == []


async def test_terminal_undelivered_payment_is_not_recharged(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _insert_payment(session_factory, status=PaymentStatus.SUCCEEDED)
    gateway = FakeGateway(PaymentStatus.FAILED)

    await PaymentProcessor(session_factory, gateway, sender).process(payment_id)

    payment = await _get(session_factory, payment_id)
    assert gateway.calls == []
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is not None
    assert _body(route)["status"] == "succeeded"


async def test_concurrent_duplicates_do_not_overwrite_status(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
) -> None:
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _insert_payment(session_factory)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
    processor = PaymentProcessor(session_factory, gateway, sender)

    await asyncio.gather(processor.process(payment_id), processor.process(payment_id))

    payment = await _get(session_factory, payment_id)
    assert 1 <= len(gateway.calls) <= 2
    assert payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
    # Only the first conditional UPDATE wins; every webhook reports the stored status.
    assert {_body(route, i)["status"] for i in range(route.call_count)} == {payment.status.value}
    assert payment.webhook_delivered_at is not None


class RecordingGateway:
    """Wraps the (idempotent) emulator and records every outcome it returns."""

    def __init__(self, inner: EmulatedGateway) -> None:
        self._inner = inner
        self.results: list[PaymentStatus] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> PaymentStatus:
        status = await self._inner.charge(payment_id, amount, currency)
        self.results.append(status)
        return status


async def test_status_update_failure_after_charge_keeps_first_gateway_outcome(
    session_factory: SessionFactory,
    clean_db: None,
    sender: WebhookSender,
    mock_http: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Double-charge window: charged, then the status UPDATE fails → redelivery re-charges.

    Seed 0 with rate 0.5: the first draw is FAILED and a fresh second draw would be SUCCEEDED, so
    only an idempotent gateway yields the same outcome on the retry.
    """
    route = mock_http.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payment_id = await _insert_payment(session_factory)
    gateway = RecordingGateway(EmulatedGateway(0, 0, 0.5, rng=random.Random(0)))
    processor = PaymentProcessor(session_factory, gateway, sender)

    original_apply = PaymentProcessor._apply_status
    failures = iter([True])

    async def flaky_apply(self: PaymentProcessor, pid: UUID, status: PaymentStatus) -> bool:
        if next(failures, False):
            raise ConnectionError("db went away after the charge")
        return await original_apply(self, pid, status)

    monkeypatch.setattr(PaymentProcessor, "_apply_status", flaky_apply)

    with pytest.raises(ConnectionError):
        await processor.process(payment_id)
    assert (await _get(session_factory, payment_id)).status == PaymentStatus.PENDING
    assert route.call_count == 0

    await processor.process(payment_id)

    payment = await _get(session_factory, payment_id)
    assert gateway.results == [PaymentStatus.FAILED, PaymentStatus.FAILED]
    assert payment.status == gateway.results[0]
    assert payment.webhook_delivered_at is not None
    assert _body(route)["status"] == "failed"
