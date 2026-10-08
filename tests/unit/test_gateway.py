import random
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import SecretStr

from payment_service.core.config import Settings
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.services.gateway import EmulatedGateway


async def _no_sleep(_: float) -> None:
    return None


async def _charge_many(gateway: EmulatedGateway, n: int) -> list[PaymentStatus]:
    return [await gateway.charge(uuid4(), Decimal("10.00"), Currency.RUB) for _ in range(n)]


async def test_success_rate_is_roughly_respected() -> None:
    gateway = EmulatedGateway(0, 0, 0.9, rng=random.Random(42), sleep=_no_sleep)
    results = await _charge_many(gateway, 1000)
    successes = results.count(PaymentStatus.SUCCEEDED)
    assert 850 <= successes <= 950
    assert set(results) <= {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


@pytest.mark.parametrize(
    ("rate", "expected"), [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)]
)
async def test_extreme_success_rates(rate: float, expected: PaymentStatus) -> None:
    gateway = EmulatedGateway(0, 0, rate, rng=random.Random(1), sleep=_no_sleep)
    assert set(await _charge_many(gateway, 200)) == {expected}


async def test_sleeps_within_configured_range() -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    gateway = EmulatedGateway(2.0, 5.0, 0.9, rng=random.Random(7), sleep=fake_sleep)
    await _charge_many(gateway, 50)
    assert len(delays) == 50
    assert all(2.0 <= d <= 5.0 for d in delays)


async def test_repeated_charge_returns_cached_outcome_without_sleeping() -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    # Seed 0, rate 0.5: first draw FAILED, a fresh second draw would be SUCCEEDED.
    gateway = EmulatedGateway(1.0, 2.0, 0.5, rng=random.Random(0), sleep=fake_sleep)
    payment_id = uuid4()

    first = await gateway.charge(payment_id, Decimal("10.00"), Currency.RUB)
    second = await gateway.charge(payment_id, Decimal("10.00"), Currency.RUB)

    assert second == first
    assert len(delays) == 1
    other = await gateway.charge(uuid4(), Decimal("10.00"), Currency.RUB)
    assert other in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
    assert len(delays) == 2


def test_from_settings() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://u:p@localhost:5432/db",
        rabbit_url="amqp://guest:guest@localhost:5672/",
        api_key=SecretStr("k"),
        gateway_min_delay_s=0,
        gateway_max_delay_s=0,
        gateway_success_rate=1.0,
    )
    assert isinstance(EmulatedGateway.from_settings(settings), EmulatedGateway)


@pytest.mark.parametrize(("lo", "hi", "rate"), [(3, 1, 0.5), (-1, 1, 0.5), (0, 1, 1.5)])
def test_rejects_invalid_parameters(lo: float, hi: float, rate: float) -> None:
    with pytest.raises(ValueError, match=r"must|require"):
        EmulatedGateway(lo, hi, rate)
