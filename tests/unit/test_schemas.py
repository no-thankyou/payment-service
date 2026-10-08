import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from payment_service.api.deps import validate_idempotency_key
from payment_service.api.errors import ApiError
from payment_service.api.schemas import MAX_METADATA_BYTES, PaymentCreate, PaymentRead
from payment_service.db.models import Payment
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.services.payments import compute_request_hash


def _body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1234",
        "metadata": {"order_id": "1234", "customer_id": 42},
        "webhook_url": "https://merchant.example.com/hooks/payments",
    }
    body.update(overrides)
    return body


def _error_locs(body: dict[str, Any]) -> list[tuple[str | int, ...]]:
    with pytest.raises(ValidationError) as exc_info:
        PaymentCreate.model_validate_json(json.dumps(body))
    return [err["loc"] for err in exc_info.value.errors()]


@pytest.mark.parametrize("amount", ["100.50", 100.5, "100.5", 100, "0.01", "9999999999999999.99"])
def test_amount_accepts_string_and_number(amount: object) -> None:
    model = PaymentCreate.model_validate_json(json.dumps(_body(amount=amount)))
    assert model.amount == Decimal(str(amount))


@pytest.mark.parametrize(
    "amount", ["0", 0, -1, "-1", "1.001", 1.001, "NaN", "Infinity", "abc", "12345678901234567.00"]
)
def test_amount_rejected(amount: object) -> None:
    assert _error_locs(_body(amount=amount)) == [("amount",)]


@pytest.mark.parametrize("currency", ["GBP", "rub", "", None])
def test_currency_rejected(currency: object) -> None:
    assert _error_locs(_body(currency=currency)) == [("currency",)]


@pytest.mark.parametrize("description", ["", "x" * 501, None])
def test_description_rejected(description: object) -> None:
    assert _error_locs(_body(description=description)) == [("description",)]


def test_missing_fields_reported() -> None:
    locs = _error_locs({})
    assert set(locs) == {("amount",), ("currency",), ("description",), ("webhook_url",)}


@pytest.mark.parametrize(
    "url", ["ftp://x.example.com/", "not-a-url", "https://", "https://e.com/" + "a" * 2048]
)
def test_webhook_url_rejected(url: str) -> None:
    assert _error_locs(_body(webhook_url=url)) == [("webhook_url",)]


def test_metadata_defaults_to_empty() -> None:
    body = _body()
    del body["metadata"]
    assert PaymentCreate.model_validate(body).metadata == {}


def test_metadata_size_limit() -> None:
    overhead = len(json.dumps({"k": ""}, separators=(",", ":")))
    fits = {"k": "x" * (MAX_METADATA_BYTES - overhead)}
    assert PaymentCreate.model_validate(_body(metadata=fits)).metadata == fits
    too_big = {"k": "x" * (MAX_METADATA_BYTES - overhead + 1)}
    assert _error_locs(_body(metadata=too_big)) == [("metadata",)]


@pytest.mark.parametrize("metadata", [[1, 2], "str", 5])
def test_metadata_must_be_object(metadata: object) -> None:
    assert _error_locs(_body(metadata=metadata)) == [("metadata",)]


def test_to_new_payment_and_hash_normalisation() -> None:
    a = PaymentCreate.model_validate(_body(amount=100.5)).to_new_payment()
    b = PaymentCreate.model_validate(_body(amount="100.50")).to_new_payment()
    assert a.currency is Currency.RUB
    assert a.webhook_url == "https://merchant.example.com/hooks/payments"
    assert isinstance(a.webhook_url, str)
    assert compute_request_hash(a) == compute_request_hash(b)


def test_payment_read_serialisation() -> None:
    now = datetime(2026, 10, 8, 12, 0, 0, 123456, tzinfo=UTC)
    payment = Payment(
        id=uuid4(),
        idempotency_key="k-1",
        request_hash="0" * 64,
        amount=Decimal("100.5"),
        currency=Currency.RUB,
        description="Order",
        metadata_={"a": 1},
        status=PaymentStatus.SUCCEEDED,
        webhook_url="https://merchant.example.com/hooks",
        webhook_attempts=1,
        webhook_delivered_at=None,
        created_at=now,
        processed_at=now,
    )
    data = PaymentRead.from_model(payment).model_dump(mode="json")
    assert data == {
        "payment_id": str(payment.id),
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order",
        "metadata": {"a": 1},
        "status": "succeeded",
        "idempotency_key": "k-1",
        "webhook_url": "https://merchant.example.com/hooks",
        "webhook_attempts": 1,
        "webhook_delivered_at": None,
        "created_at": "2026-10-08T12:00:00.123456Z",
        "processed_at": "2026-10-08T12:00:00.123456Z",
    }


@pytest.mark.parametrize("key", ["a", "order-1234 attempt 1", "~" * 255, " x "])
def test_idempotency_key_valid(key: str) -> None:
    assert validate_idempotency_key(key) == key


@pytest.mark.parametrize("key", [None, "", "   "])
def test_idempotency_key_missing(key: str | None) -> None:
    with pytest.raises(ApiError) as exc_info:
        validate_idempotency_key(key)
    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "idempotency_key_missing"


@pytest.mark.parametrize("key", ["a" * 256, "tab\tkey", "ключ", "x\x7f"])
def test_idempotency_key_invalid(key: str) -> None:
    with pytest.raises(ApiError) as exc_info:
        validate_idempotency_key(key)
    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "idempotency_key_invalid"
