import pytest
from pydantic import SecretStr

from payment_service.broker.topology import (
    NEW_ROUTING_KEY,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_QUEUE,
    retry_delay_for_attempt,
    retry_queue,
    retry_routing_key,
)
from payment_service.core.config import Settings


def test_retry_queue_naming_and_args() -> None:
    queue = retry_queue(1000)
    assert queue.name == "payments.retry.1000ms"
    assert queue.routing() == retry_routing_key(1000) == "1000ms"
    assert queue.durable is True
    assert queue.arguments is not None
    assert queue.arguments["x-message-ttl"] == 1000
    assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE_NAME
    assert queue.arguments["x-dead-letter-routing-key"] == NEW_ROUTING_KEY


def test_retry_queue_rejects_non_positive_delay() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        retry_queue(0)


def test_new_queue_dead_letters_to_dlq() -> None:
    assert PAYMENTS_NEW_QUEUE.routing() == "payments.new"
    assert PAYMENTS_NEW_QUEUE.arguments is not None
    assert PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-routing-key"] == "payments.dlq"


def test_retry_delay_for_attempt() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://u:p@localhost:5432/db",
        rabbit_url="amqp://guest:guest@localhost:5672/",
        api_key=SecretStr("k"),
        retry_max_attempts=3,
        retry_base_delay_ms=1000,
    )
    assert retry_delay_for_attempt(settings, 1) == 1000
    assert retry_delay_for_attempt(settings, 2) == 2000
    with pytest.raises(ValueError, match="last one"):
        retry_delay_for_attempt(settings, 3)
    with pytest.raises(ValueError, match="must be >= 1"):
        retry_delay_for_attempt(settings, 0)
