"""Emulated payment gateway.

A gateway decline is a business outcome (``PaymentStatus.FAILED``), returned as a value and never
raised: a failed payment is terminal and must not be retried.
"""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Protocol, Self
from uuid import UUID

from payment_service.core.config import Settings
from payment_service.domain.enums import Currency, PaymentStatus

logger = logging.getLogger(__name__)

SleepFn = Callable[[float], Awaitable[None]]


class PaymentGateway(Protocol):
    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> PaymentStatus:
        """Charge the payment; return ``SUCCEEDED`` or ``FAILED`` (decline is not an error).

        Contract: idempotent per ``payment_id``. A real gateway receives ``payment_id`` as its
        idempotency key and must return the same outcome for repeated calls without charging
        again. This closes the window between a successful charge and the consumer's status
        ``UPDATE`` (crash / DB error in between → redelivery charges again).
        """
        ...


class EmulatedGateway:
    """Sleeps a random delay, then succeeds with probability ``success_rate``.

    Idempotent per ``payment_id`` like a real gateway: the first outcome is remembered and a
    repeated call returns it immediately, without sleeping or drawing a new random outcome. The
    memory is per process (emulation only); a real gateway keeps it on its side.
    """

    def __init__(
        self,
        min_delay_s: float,
        max_delay_s: float,
        success_rate: float,
        rng: random.Random | None = None,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        if min_delay_s < 0 or min_delay_s > max_delay_s:
            raise ValueError("require 0 <= min_delay_s <= max_delay_s")
        if not 0.0 <= success_rate <= 1.0:
            raise ValueError("success_rate must be within [0, 1]")
        self._min_delay_s = min_delay_s
        self._max_delay_s = max_delay_s
        self._success_rate = success_rate
        # Emulation only, not cryptography: a seedable PRNG is what we want here.
        self._rng = rng if rng is not None else random.Random()  # noqa: S311
        self._sleep = sleep
        self._results: dict[UUID, PaymentStatus] = {}

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            min_delay_s=settings.gateway_min_delay_s,
            max_delay_s=settings.gateway_max_delay_s,
            success_rate=settings.gateway_success_rate,
        )

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> PaymentStatus:
        cached = self._results.get(payment_id)
        if cached is not None:
            logger.info("gateway charge replayed payment_id=%s -> %s", payment_id, cached.value)
            return cached
        delay = self._rng.uniform(self._min_delay_s, self._max_delay_s)
        await self._sleep(delay)
        succeeded = self._rng.random() < self._success_rate
        status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        # setdefault: if a concurrent call for the same id finished first, its outcome wins.
        status = self._results.setdefault(payment_id, status)
        logger.info(
            "gateway charge payment_id=%s amount=%s %s delay=%.3fs -> %s",
            payment_id,
            amount,
            currency.value,
            delay,
            status.value,
        )
        return status
