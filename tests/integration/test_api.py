import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.api.main import create_app
from payment_service.api.routes.payments import MAX_BODY_BYTES
from payment_service.core.config import Settings
from payment_service.db.models import OutboxEvent, Payment
from payment_service.db.session import create_engine, create_session_factory

pytestmark = pytest.mark.integration

API_KEY = "test-api-key"
SessionFactory = async_sessionmaker[AsyncSession]


def _settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        rabbit_url="amqp://guest:guest@localhost:5672/",  # unused: the api never talks to RabbitMQ
        api_key=SecretStr(API_KEY),
    )


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


def _headers(
    idem: str | bytes | None = "order-1234-attempt-1", api_key: str | None = API_KEY
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if api_key is not None:
        headers["X-API-Key"] = api_key
    if idem is not None:
        headers["Idempotency-Key"] = idem
    return headers


@pytest.fixture
async def client(
    migrated_db: str, session_factory: SessionFactory, clean_db: None
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(_settings(migrated_db), session_factory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _count(session_factory: SessionFactory, model: type[Payment | OutboxEvent]) -> int:
    async with session_factory() as session:
        return (await session.scalar(select(func.count()).select_from(model))) or 0


def _assert_error(resp: httpx.Response, status: int, code: str) -> None:
    assert resp.status_code == status, resp.text
    detail = resp.json()["detail"]
    assert detail["code"] == code
    assert isinstance(detail["message"], str)
    assert detail["message"]


async def test_create_payment_accepted(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    resp = await client.post("/api/v1/payments", json=_body(amount=100.5), headers=_headers())

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert "Idempotent-Replayed" not in resp.headers
    async with session_factory() as session:
        payment = await session.get_one(Payment, UUID(data["payment_id"]))
    assert str(payment.amount) == "100.50"
    assert await _count(session_factory, OutboxEvent) == 1

    got = await client.get(f"/api/v1/payments/{data['payment_id']}", headers=_headers(None))
    assert got.json()["amount"] == "100.50"


async def test_replay_returns_same_payment(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    first = await client.post("/api/v1/payments", json=_body(), headers=_headers())
    # Same canonical body: amount as number, different key order.
    second = await client.post(
        "/api/v1/payments", json=dict(reversed(_body(amount=100.5).items())), headers=_headers()
    )

    assert first.status_code == second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_reused_key_with_different_body(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    await client.post("/api/v1/payments", json=_body(), headers=_headers())
    resp = await client.post("/api/v1/payments", json=_body(amount="1.00"), headers=_headers())

    _assert_error(resp, 422, "idempotency_key_reused")
    assert await _count(session_factory, Payment) == 1


@pytest.mark.parametrize("api_key", [None, "wrong-key", ""])
async def test_post_unauthorized(client: httpx.AsyncClient, api_key: str | None) -> None:
    resp = await client.post("/api/v1/payments", json=_body(), headers=_headers(api_key=api_key))
    _assert_error(resp, 401, "unauthorized")


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
async def test_get_unauthorized(client: httpx.AsyncClient, api_key: str | None) -> None:
    resp = await client.get(f"/api/v1/payments/{uuid4()}", headers=_headers(None, api_key))
    _assert_error(resp, 401, "unauthorized")


@pytest.mark.parametrize(
    ("idem", "code"),
    [
        (None, "idempotency_key_missing"),
        ("", "idempotency_key_missing"),
        ("   ", "idempotency_key_missing"),
        ("k" * 256, "idempotency_key_invalid"),
        ("ключ".encode(), "idempotency_key_invalid"),
        ("tab\tkey", "idempotency_key_invalid"),
    ],
)
async def test_idempotency_key_errors(
    client: httpx.AsyncClient, idem: str | bytes | None, code: str
) -> None:
    resp = await client.post("/api/v1/payments", json=_body(), headers=_headers(idem))
    _assert_error(resp, 400, code)


async def test_idempotency_key_max_length_ok(client: httpx.AsyncClient) -> None:
    resp = await client.post("/api/v1/payments", json=_body(), headers=_headers("k" * 255))
    assert resp.status_code == 202, resp.text


async def test_precedence_401_before_400_before_422(client: httpx.AsyncClient) -> None:
    bad_json = b"{not json"
    json_ct = {"Content-Type": "application/json"}
    invalid = _body(amount=0)

    resp = await client.post("/api/v1/payments", json=invalid, headers=_headers(None, None))
    _assert_error(resp, 401, "unauthorized")
    resp = await client.post(
        "/api/v1/payments",
        content=bad_json,
        headers={**_headers(None, None), **json_ct},
    )
    _assert_error(resp, 401, "unauthorized")

    resp = await client.post("/api/v1/payments", json=invalid, headers=_headers(None))
    _assert_error(resp, 400, "idempotency_key_missing")
    resp = await client.post(
        "/api/v1/payments",
        content=bad_json,
        headers={**_headers(None), **json_ct},
    )
    _assert_error(resp, 400, "idempotency_key_missing")

    resp = await client.post("/api/v1/payments", json=invalid, headers=_headers())
    assert resp.status_code == 422
    assert isinstance(resp.json()["detail"], list)


@pytest.mark.parametrize(
    ("overrides", "loc"),
    [
        ({"amount": 0}, ["body", "amount"]),
        ({"amount": "0"}, ["body", "amount"]),
        ({"amount": -1}, ["body", "amount"]),
        ({"amount": 1.001}, ["body", "amount"]),
        ({"amount": "1.001"}, ["body", "amount"]),
        ({"currency": "GBP"}, ["body", "currency"]),
        ({"webhook_url": "ftp://x"}, ["body", "webhook_url"]),
        ({"metadata": {"blob": "x" * 16384}}, ["body", "metadata"]),
        ({"description": None}, ["body", "description"]),
        ({"description": ""}, ["body", "description"]),
    ],
)
async def test_validation_errors(
    client: httpx.AsyncClient,
    session_factory: SessionFactory,
    overrides: dict[str, Any],
    loc: list[str],
) -> None:
    body = _body(**overrides)
    if body.get("description") is None:
        del body["description"]
    resp = await client.post("/api/v1/payments", json=body, headers=_headers())

    assert resp.status_code == 422, resp.text
    errors = resp.json()["detail"]
    assert [err["loc"] for err in errors] == [loc]
    assert {"type", "msg"} <= set(errors[0])
    assert await _count(session_factory, Payment) == 0


@pytest.mark.parametrize("content", [b"{not json", b"", b"[]"])
async def test_malformed_body(client: httpx.AsyncClient, content: bytes) -> None:
    resp = await client.post(
        "/api/v1/payments",
        content=content,
        headers={**_headers(), "Content-Type": "application/json"},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"][0]["loc"][0] == "body"


@pytest.mark.parametrize("content_type", [None, "text/plain", "application/x-www-form-urlencoded"])
async def test_unsupported_media_type(
    client: httpx.AsyncClient, session_factory: SessionFactory, content_type: str | None
) -> None:
    headers = _headers()
    if content_type is not None:
        headers["Content-Type"] = content_type
    resp = await client.post("/api/v1/payments", content=json.dumps(_body()), headers=headers)
    _assert_error(resp, 415, "unsupported_media_type")
    assert await _count(session_factory, Payment) == 0


async def test_json_content_type_with_params_is_accepted(client: httpx.AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/payments",
        content=json.dumps(_body()),
        headers={**_headers(), "Content-Type": "Application/JSON; charset=utf-8"},
    )
    assert resp.status_code == 202, resp.text


def _oversized_body() -> bytes:
    return json.dumps(_body(description="x" * MAX_BODY_BYTES)).encode()


async def test_payload_too_large_by_content_length(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    resp = await client.post(
        "/api/v1/payments",
        content=_oversized_body(),
        headers={**_headers(), "Content-Type": "application/json"},
    )
    _assert_error(resp, 413, "payload_too_large")
    assert await _count(session_factory, Payment) == 0


async def test_payload_too_large_by_actual_length(client: httpx.AsyncClient) -> None:
    body = _oversized_body()

    async def chunks() -> AsyncIterator[bytes]:  # streamed: no Content-Length header
        for start in range(0, len(body), 8192):
            yield body[start : start + 8192]

    resp = await client.post(
        "/api/v1/payments",
        content=chunks(),
        headers={**_headers(), "Content-Type": "application/json"},
    )
    _assert_error(resp, 413, "payload_too_large")


async def test_precedence_401_400_before_415_413(client: httpx.AsyncClient) -> None:
    big = {"Content-Type": "application/json"}
    text_ct = {"Content-Type": "text/plain"}
    resp = await client.post(
        "/api/v1/payments", content=b"x", headers={**_headers(None, None), **text_ct}
    )
    _assert_error(resp, 401, "unauthorized")
    resp = await client.post(
        "/api/v1/payments", content=b"x", headers={**_headers(None), **text_ct}
    )
    _assert_error(resp, 400, "idempotency_key_missing")
    resp = await client.post(
        "/api/v1/payments", content=_oversized_body(), headers={**_headers(None, None), **big}
    )
    _assert_error(resp, 401, "unauthorized")
    resp = await client.post(
        "/api/v1/payments", content=_oversized_body(), headers={**_headers(None), **big}
    )
    _assert_error(resp, 400, "idempotency_key_missing")
    resp = await client.post(
        "/api/v1/payments", content=_oversized_body(), headers={**_headers(), **text_ct}
    )
    _assert_error(resp, 415, "unsupported_media_type")


async def test_get_payment(client: httpx.AsyncClient) -> None:
    created = await client.post("/api/v1/payments", json=_body(), headers=_headers())
    payment_id = created.json()["payment_id"]

    resp = await client.get(f"/api/v1/payments/{payment_id}", headers=_headers(None))

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data == {
        "payment_id": payment_id,
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1234",
        "metadata": {"order_id": "1234", "customer_id": 42},
        "status": "pending",
        "idempotency_key": "order-1234-attempt-1",
        "webhook_url": "https://merchant.example.com/hooks/payments",
        "webhook_attempts": 0,
        "webhook_delivered_at": None,
        "created_at": created.json()["created_at"],
        "processed_at": None,
    }
    assert data["created_at"].endswith("Z")


async def test_get_unknown_payment(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/api/v1/payments/{uuid4()}", headers=_headers(None))
    _assert_error(resp, 404, "payment_not_found")


async def test_get_malformed_uuid(client: httpx.AsyncClient) -> None:
    resp = await client.get("/api/v1/payments/not-a-uuid", headers=_headers(None))
    assert resp.status_code == 422
    assert resp.json()["detail"][0]["loc"] == ["path", "payment_id"]


async def test_health_ok_without_api_key(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_health_unavailable() -> None:
    url = "postgresql+asyncpg://u:p@127.0.0.1:1/none"
    engine = create_engine(url)
    try:
        app = create_app(_settings(url), create_session_factory(engine))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            resp = await c.get("/health")
    finally:
        await engine.dispose()
    assert resp.status_code == 503
    assert resp.json() == {"status": "unavailable"}


async def test_openapi_documents_headers(client: httpx.AsyncClient) -> None:
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    text = resp.text
    assert "Idempotency-Key" in text
    assert "X-API-Key" in text
    post = schema["paths"]["/api/v1/payments"]["post"]
    params = {p["name"]: p for p in post["parameters"]}
    assert params["Idempotency-Key"]["required"] is True
    assert "Idempotent-Replayed" in post["responses"]["202"]["headers"]
    assert {"400", "401", "422"} <= set(post["responses"])
    assert {"401", "404", "422"} <= set(
        schema["paths"]["/api/v1/payments/{payment_id}"]["get"]["responses"]
    )
    body_schema = post["requestBody"]["content"]["application/json"]["schema"]
    currency_ref = body_schema["properties"]["currency"]["$ref"]
    assert currency_ref.removeprefix("#/components/schemas/") in schema["components"]["schemas"]
