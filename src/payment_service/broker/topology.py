"""RabbitMQ topology: the only place that knows exchange / queue / routing-key names.

payments (direct)       --"payments.new"--> payments.new   [DLX=payments.dlx, dl-rk=payments.dlq]
payments.retry (direct) --"{d}ms"--> payments.retry.{d}ms [TTL=d, DLX=payments, dl-rk=payments.new]
payments.dlx (direct)   --"payments.dlq"--> payments.dlq

Retry queues have no consumers: messages expire after the TTL and are dead-lettered back to
``payments.new``. The delay is part of the queue name, so changing the backoff config creates new
queues instead of failing with PRECONDITION_FAILED on re-declaration.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payment_service.core.config import Settings

PAYMENTS_EXCHANGE_NAME = "payments"
RETRY_EXCHANGE_NAME = "payments.retry"
DLX_EXCHANGE_NAME = "payments.dlx"

NEW_QUEUE_NAME = "payments.new"
NEW_ROUTING_KEY = "payments.new"
DLQ_QUEUE_NAME = "payments.dlq"
DLQ_ROUTING_KEY = "payments.dlq"

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange(DLX_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    NEW_QUEUE_NAME,
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)
DLQ_QUEUE = RabbitQueue(DLQ_QUEUE_NAME, durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_routing_key(delay_ms: int) -> str:
    return f"{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    """Delay queue: holds messages for ``delay_ms`` and then dead-letters them to payments.new."""
    if delay_ms <= 0:
        raise ValueError(f"delay_ms must be positive, got {delay_ms}")
    return RabbitQueue(
        f"{RETRY_EXCHANGE_NAME}.{delay_ms}ms",
        durable=True,
        routing_key=retry_routing_key(delay_ms),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )


def retry_delay_for_attempt(settings: Settings, attempt: int) -> int:
    """Delay (ms) before the next delivery after failed delivery number ``attempt`` (1-based).

    Raises ValueError when ``attempt`` is out of range, in particular when it is the last allowed
    attempt (``attempt >= retry_max_attempts``): such a message goes to the DLQ, not to a retry.
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    if attempt >= settings.retry_max_attempts:
        raise ValueError(
            f"attempt {attempt} is the last one (max_attempts={settings.retry_max_attempts})"
        )
    return settings.retry_delays_ms()[attempt - 1]


async def _declare_bound(
    broker: RabbitBroker, queue: RabbitQueue, exchange: RabbitExchange
) -> None:
    robust_exchange = await broker.declare_exchange(exchange)
    robust_queue = await broker.declare_queue(queue)
    await robust_queue.bind(robust_exchange, routing_key=queue.routing())


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Declare all exchanges, queues and bindings. Idempotent; connects the broker if needed."""
    await broker.connect()
    await _declare_bound(broker, PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE)
    await _declare_bound(broker, DLQ_QUEUE, DLX_EXCHANGE)
    await broker.declare_exchange(RETRY_EXCHANGE)
    for delay_ms in settings.retry_delays_ms():
        await _declare_bound(broker, retry_queue(delay_ms), RETRY_EXCHANGE)
