"""``PaymentService`` business rules against in-memory repositories (no database)."""

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from payment_service.broker.topology import NEW_ROUTING_KEY, PAYMENTS_EXCHANGE_NAME
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError
from payment_service.domain.events import PAYMENT_CREATED
from payment_service.domain.models import NewPayment, OutboxMessage
from payment_service.services.payments import PaymentService, compute_request_hash
from tests.unit.fakes import FakeUnitOfWork, InMemoryOutboxRepository, InMemoryStore


def _data(amount: str = "100.50") -> NewPayment:
    return NewPayment(
        amount=Decimal(amount),
        currency=Currency.RUB,
        description="Order #1234",
        metadata={"order_id": "1234"},
        webhook_url="https://merchant.example.com/hooks/payments",
    )


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
def service(store: InMemoryStore) -> PaymentService:
    return PaymentService(lambda: FakeUnitOfWork(store))


async def test_create_stores_payment_and_outbox_message(
    service: PaymentService, store: InMemoryStore
) -> None:
    data = _data()
    result = await service.create("key-1", data)

    assert result.replayed is False
    assert result.payment.status is PaymentStatus.PENDING
    assert result.payment.request_hash == compute_request_hash(data)
    assert list(store.payments) == [result.payment.id]
    (message,) = store.outbox.values()
    assert message.event_type == PAYMENT_CREATED
    assert message.exchange == PAYMENTS_EXCHANGE_NAME
    assert message.routing_key == NEW_ROUTING_KEY
    assert UUID(message.payload["payment_id"]) == result.payment.id
    assert UUID(message.payload["event_id"]) == message.id


async def test_replay_with_same_body_writes_nothing(
    service: PaymentService, store: InMemoryStore
) -> None:
    first = await service.create("key-1", _data())
    second = await service.create("key-1", _data("100.5"))

    assert second.replayed is True
    assert second.payment == first.payment
    assert len(store.outbox) == 1
    assert store.commits == 1


async def test_same_key_with_different_body_is_rejected(
    service: PaymentService, store: InMemoryStore
) -> None:
    await service.create("key-1", _data())
    with pytest.raises(IdempotencyKeyReusedError):
        await service.create("key-1", _data("200.00"))
    assert len(store.payments) == 1
    assert len(store.outbox) == 1


async def test_payment_is_not_stored_without_its_outbox_message(store: InMemoryStore) -> None:
    class BrokenOutbox(InMemoryOutboxRepository):
        async def add(self, message: OutboxMessage) -> None:
            raise ConnectionError("db went away")

    def uow_factory() -> FakeUnitOfWork:
        uow = FakeUnitOfWork(store)
        uow.outbox = BrokenOutbox(store)
        return uow

    with pytest.raises(ConnectionError):
        await PaymentService(uow_factory).create("key-1", _data())
    assert store.payments == {}
    assert store.commits == 0


async def test_get(service: PaymentService) -> None:
    created = await service.create("key-1", _data())
    assert await service.get(created.payment.id) == created.payment


async def test_get_unknown_raises_not_found(service: PaymentService) -> None:
    unknown = uuid4()
    with pytest.raises(PaymentNotFoundError) as exc_info:
        await service.get(unknown)
    assert exc_info.value.payment_id == unknown
