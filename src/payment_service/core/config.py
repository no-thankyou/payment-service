from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Required, no defaults: a misconfigured deployment must fail fast instead of silently using
    # a well-known dev key or localhost URLs.
    database_url: str
    rabbit_url: str
    api_key: SecretStr

    retry_max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_ms: int = Field(default=1000, ge=1)

    outbox_poll_interval_s: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)

    gateway_min_delay_s: float = Field(default=2.0, ge=0)
    gateway_max_delay_s: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout_s: float = Field(default=5.0, gt=0)
    consumer_prefetch: int = Field(default=10, ge=1)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_gateway_delays(self) -> "Settings":
        if self.gateway_min_delay_s > self.gateway_max_delay_s:
            raise ValueError("gateway_min_delay_s must be <= gateway_max_delay_s")
        return self

    def retry_delays_ms(self) -> list[int]:
        """Exponential backoff before retry N (1-based): base * 2**(N-1)."""
        return [self.retry_base_delay_ms * 2**i for i in range(self.retry_max_attempts - 1)]


@lru_cache
def get_settings() -> Settings:
    return Settings()
