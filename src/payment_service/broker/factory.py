import logging

from faststream.rabbit import Channel, RabbitBroker

from payment_service.core.config import Settings


def make_broker(settings: Settings) -> RabbitBroker:
    """RabbitBroker with prefetch set on the default channel (used by subscribers and publishers).

    Publisher confirms are on by default (``Channel.publisher_confirms=True``).
    """
    log_level = logging.getLevelNamesMapping().get(settings.log_level.upper(), logging.INFO)
    return RabbitBroker(
        settings.rabbit_url,
        default_channel=Channel(prefetch_count=settings.consumer_prefetch),
        log_level=log_level,
    )
