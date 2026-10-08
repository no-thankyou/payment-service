from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from payment_service.core.config import Settings

REQUIRED: dict[str, Any] = {
    "database_url": "postgresql+asyncpg://u:p@localhost:5432/db",
    "rabbit_url": "amqp://guest:guest@localhost:5672/",
    "api_key": SecretStr("k"),
}


def test_defaults() -> None:
    s = Settings(_env_file=None, **REQUIRED)
    assert s.retry_max_attempts == 3
    assert s.retry_delays_ms() == [1000, 2000]
    assert s.gateway_success_rate == 0.9


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "4")
    monkeypatch.setenv("RETRY_BASE_DELAY_MS", "100")
    monkeypatch.setenv("API_KEY", "secret")
    monkeypatch.setenv("DATABASE_URL", REQUIRED["database_url"])
    monkeypatch.setenv("RABBIT_URL", REQUIRED["rabbit_url"])
    s = Settings(_env_file=None)
    assert s.retry_delays_ms() == [100, 200, 400]
    assert s.api_key.get_secret_value() == "secret"
    assert s.database_url == REQUIRED["database_url"]


@pytest.mark.parametrize("missing", ["API_KEY", "DATABASE_URL", "RABBIT_URL"])
def test_required_settings_have_no_defaults(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    for name in ("API_KEY", "DATABASE_URL", "RABBIT_URL"):
        monkeypatch.setenv(name, "x")
    monkeypatch.delenv(missing)
    with pytest.raises(ValidationError, match=missing.lower()):
        Settings(_env_file=None)


def test_single_attempt_has_no_retry_delays() -> None:
    assert Settings(_env_file=None, retry_max_attempts=1, **REQUIRED).retry_delays_ms() == []


def test_invalid_gateway_delays() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, gateway_min_delay_s=5, gateway_max_delay_s=2, **REQUIRED)
