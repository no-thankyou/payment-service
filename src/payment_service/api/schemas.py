"""Request/response models of the HTTP API."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    UrlConstraints,
    field_validator,
)

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.models import NewPayment, Payment

MAX_METADATA_BYTES = 16384
MAX_WEBHOOK_URL_LENGTH = 2048
_CENTS = Decimal("0.01")


def _money_to_str(value: Decimal) -> str:
    return str(value.quantize(_CENTS))


#: Decimal serialised as a 2-decimal JSON string ("100.50").
MoneyOut = Annotated[Decimal, PlainSerializer(_money_to_str, return_type=str, when_used="always")]

WebhookUrl = Annotated[
    AnyUrl,
    UrlConstraints(
        max_length=MAX_WEBHOOK_URL_LENGTH, allowed_schemes=["http", "https"], host_required=True
    ),
]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "examples": [
                {
                    "amount": "100.50",
                    "currency": "RUB",
                    "description": "Order #1234",
                    "metadata": {"order_id": "1234", "customer_id": 42},
                    "webhook_url": "https://merchant.example.com/hooks/payments",
                }
            ]
        },
    )

    amount: Decimal = Field(
        gt=0,
        max_digits=18,
        decimal_places=2,
        description="Positive amount, at most 2 decimal places; JSON string or number.",
    )
    currency: Currency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, Any] = Field(
        default_factory=dict, description=f"Arbitrary JSON object, <= {MAX_METADATA_BYTES} bytes."
    )
    webhook_url: WebhookUrl

    @field_validator("metadata")
    @classmethod
    def _metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        size = len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        if size > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must be at most {MAX_METADATA_BYTES} bytes as JSON")
        return value

    def to_new_payment(self) -> NewPayment:
        return NewPayment(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=str(self.webhook_url),
        )


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_domain(cls, payment: Payment) -> "PaymentAccepted":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentRead(BaseModel):
    payment_id: UUID
    amount: MoneyOut
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    webhook_attempts: int
    webhook_delivered_at: datetime | None
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_domain(cls, payment: Payment) -> "PaymentRead":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            webhook_attempts=payment.webhook_attempts,
            webhook_delivered_at=payment.webhook_delivered_at,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorBody(BaseModel):
    """Application error body: ``{"detail": {"code": ..., "message": ...}}``."""

    detail: ErrorDetail


class HealthStatus(BaseModel):
    status: str
