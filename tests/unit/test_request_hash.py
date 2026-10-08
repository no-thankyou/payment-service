from dataclasses import replace
from decimal import Decimal

from payment_service.domain.enums import Currency
from payment_service.services.payments import NewPayment, compute_request_hash


def _data(**overrides: object) -> NewPayment:
    base = NewPayment(
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Order #1234",
        metadata={"order_id": "1234", "customer_id": 42},
        webhook_url="https://merchant.example.com/hooks/payments",
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


def test_same_data_same_hash() -> None:
    h = compute_request_hash(_data())
    assert h == compute_request_hash(_data())
    assert len(h) == 64
    int(h, 16)


def test_amount_normalised_to_two_decimals() -> None:
    assert compute_request_hash(_data(amount=Decimal("100.5"))) == compute_request_hash(
        _data(amount=Decimal("100.50"))
    )


def test_metadata_key_order_irrelevant() -> None:
    a = _data(metadata={"a": 1, "b": {"x": 1, "y": 2}})
    b = _data(metadata={"b": {"y": 2, "x": 1}, "a": 1})
    assert compute_request_hash(a) == compute_request_hash(b)


def test_differences_change_hash() -> None:
    base = compute_request_hash(_data())
    variants = [
        _data(amount=Decimal("100.51")),
        _data(currency=Currency.USD),
        _data(description="Order #1235"),
        _data(metadata={"order_id": "1235", "customer_id": 42}),
        _data(metadata={}),
        _data(webhook_url="https://merchant.example.com/hooks/other"),
    ]
    hashes = {compute_request_hash(v) for v in variants}
    assert base not in hashes
    assert len(hashes) == len(variants)


def test_non_ascii_metadata_supported() -> None:
    a = compute_request_hash(_data(metadata={"note": "заказ"}))
    b = compute_request_hash(_data(metadata={"note": "заказ"}))
    assert a == b
