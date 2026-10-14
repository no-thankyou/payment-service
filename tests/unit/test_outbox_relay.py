"""``OutboxRelay`` batch logic against an in-memory outbox and a stub broker."""

from typing import Any, cast
from uuid import uuid4

from faststream.rabbit import RabbitBroker

from payment_service.domain.models import OutboxMessage
from payment_service.outbox.relay import LAST_ERROR_MAX_LEN, OutboxRelay
from tests.unit.fakes import FakeUnitOfWork, InMemoryStore


class StubBroker:
    """Records published message ids; fails the calls listed in ``fail_on`` (1-based)."""

    def __init__(self, fail_on: tuple[int, ...] = (), error: str = "broker unavailable") -> None:
        self.fail_on = fail_on
        self.error = error
        self.calls: list[str] = []

    async def publish(self, message: Any, **kwargs: Any) -> None:
        self.calls.append(kwargs["message_id"])
        if len(self.calls) in self.fail_on:
            raise ConnectionError(self.error)


def _seed(store: InMemoryStore, count: int) -> list[OutboxMessage]:
    messages = [
        OutboxMessage(
            id=uuid4(),
            event_type="payment.created",
            exchange="payments",
            routing_key="payments.new",
            payload={"seq": i},
        )
        for i in range(count)
    ]
    store.outbox.update((m.id, m) for m in messages)
    return messages


def _relay(store: InMemoryStore, broker: StubBroker, batch_size: int = 10) -> OutboxRelay:
    return OutboxRelay(
        lambda: FakeUnitOfWork(store), cast(RabbitBroker, broker), batch_size, poll_interval_s=0
    )


async def test_publishes_a_batch_and_marks_it_published() -> None:
    store = InMemoryStore()
    messages = _seed(store, 3)
    broker = StubBroker()

    assert await _relay(store, broker, batch_size=2).run_once() == 2

    assert broker.calls == [str(m.id) for m in messages[:2]]
    assert store.published == {m.id for m in messages[:2]}


async def test_failure_stops_the_batch_and_records_the_error() -> None:
    store = InMemoryStore()
    first, second, third = _seed(store, 3)
    broker = StubBroker(fail_on=(2,), error="x" * (LAST_ERROR_MAX_LEN + 10))

    assert await _relay(store, broker).run_once() == 1

    assert broker.calls == [str(first.id), str(second.id)]
    assert store.published == {first.id}
    assert store.outbox[second.id].attempts == 1
    assert store.errors[second.id] == "x" * LAST_ERROR_MAX_LEN
    assert store.outbox[third.id].attempts == 0
    assert store.commits == 1
