"""Consumer service: `payments.new` subscriber + outbox relay task.

Run: ``faststream run --factory payment_service.consumer.main:app_factory``.
Nothing is created at import time; all resources live in ``_ConsumerRuntime``.
"""

import asyncio
import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.broker.factory import make_broker
from payment_service.broker.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, declare_topology
from payment_service.consumer.handler import PaymentEventHandler
from payment_service.core.config import Settings, get_settings
from payment_service.core.logging import configure_logging
from payment_service.db.session import create_engine, create_session_factory
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.outbox.relay import OutboxRelay
from payment_service.services.gateway import EmulatedGateway, PaymentGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhook import WebhookSender

logger = logging.getLogger(__name__)

RELAY_STOP_TIMEOUT_S = 15.0


class _ConsumerRuntime:
    """Resources of one consumer app. Closes only what it created itself."""

    def __init__(
        self,
        settings: Settings,
        broker: RabbitBroker,
        session_factory: async_sessionmaker[AsyncSession] | None,
        gateway: PaymentGateway | None,
        http_client: httpx.AsyncClient | None,
    ) -> None:
        self.settings = settings
        self.broker = broker
        self._injected_session_factory = session_factory
        self._injected_gateway = gateway
        self._injected_http_client = http_client

        self.session_factory: async_sessionmaker[AsyncSession] | None = None
        self.handler: PaymentEventHandler | None = None
        self._own_engine: AsyncEngine | None = None
        self._own_http_client: httpx.AsyncClient | None = None
        self._stop = asyncio.Event()
        self._relay_task: asyncio.Task[None] | None = None

    async def on_startup(self) -> None:
        configure_logging(self.settings.log_level)
        self._stop = asyncio.Event()

        if self._injected_session_factory is not None:
            self.session_factory = self._injected_session_factory
        else:
            self._own_engine = create_engine(self.settings.database_url)
            self.session_factory = create_session_factory(self._own_engine)

        http_client = self._injected_http_client
        if http_client is None:
            http_client = self._own_http_client = httpx.AsyncClient()

        gateway = self._injected_gateway or EmulatedGateway.from_settings(self.settings)
        webhook = WebhookSender(http_client, timeout_s=self.settings.webhook_timeout_s)
        processor = PaymentProcessor(self.session_factory, gateway, webhook)
        self.handler = PaymentEventHandler(self.settings, self.broker, processor)

        # Declared before the subscriber starts consuming, so the DLX/DLQ and retry queues exist
        # before the first message can be rejected or retried.
        await declare_topology(self.broker, self.settings)

    async def after_startup(self) -> None:
        if self.session_factory is None:  # pragma: no cover - on_startup always runs first
            raise RuntimeError("consumer runtime is not started")
        relay = OutboxRelay(
            self.session_factory,
            self.broker,
            batch_size=self.settings.outbox_batch_size,
            poll_interval_s=self.settings.outbox_poll_interval_s,
        )
        self._relay_task = asyncio.create_task(relay.run_forever(self._stop), name="outbox-relay")

    async def on_shutdown(self) -> None:
        self._stop.set()
        if self._relay_task is not None:
            try:
                await asyncio.wait_for(self._relay_task, timeout=RELAY_STOP_TIMEOUT_S)
            except TimeoutError:
                logger.warning("outbox relay did not stop in %ss, cancelled", RELAY_STOP_TIMEOUT_S)
            self._relay_task = None

    async def after_shutdown(self) -> None:
        # After the broker is stopped: in-flight handlers may still use the HTTP client / DB.
        if self._own_http_client is not None:
            await self._own_http_client.aclose()
            self._own_http_client = None
        if self._own_engine is not None:
            await self._own_engine.dispose()
            self._own_engine = None


def create_consumer(
    settings: Settings,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    gateway: PaymentGateway | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastStream:
    """Build the consumer app. Injected dependencies are used as-is and never closed here."""
    broker = make_broker(settings)
    runtime = _ConsumerRuntime(settings, broker, session_factory, gateway, http_client)

    @broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def on_payment_created(event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
        if runtime.handler is None:  # pragma: no cover - subscriber starts after on_startup
            raise RuntimeError("consumer runtime is not started")
        await runtime.handler.handle(event, msg)

    return FastStream(
        broker,
        on_startup=[runtime.on_startup],
        after_startup=[runtime.after_startup],
        on_shutdown=[runtime.on_shutdown],
        after_shutdown=[runtime.after_shutdown],
    )


def app_factory() -> FastStream:
    return create_consumer(get_settings())
