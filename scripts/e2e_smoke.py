"""End-to-end smoke test against the running stack (`make up-dev`, then `make e2e`).

Exercises the README scenario: auth, idempotency, async processing + webhook, retries + DLQ.
Exits non-zero on the first failed check. Safe to re-run (unique idempotency keys).
"""

import os
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any, NoReturn

import httpx

API = os.environ.get("E2E_API_URL", "http://localhost:8000")
SINK = os.environ.get("E2E_SINK_URL", "http://localhost:8081")
SINK_INTERNAL = os.environ.get("E2E_SINK_INTERNAL_URL", "http://webhook-sink:8080")
RABBIT_MGMT = os.environ.get("E2E_RABBIT_MGMT_URL", "http://localhost:15672")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
TIMEOUT_S = 30.0


def fail(msg: str) -> NoReturn:
    print(f"FAIL: {msg}")
    sys.exit(1)


def check(cond: bool, msg: str) -> None:
    if not cond:
        fail(msg)
    print(f"ok   {msg}")


def wait_for(fn: Callable[[], Any], what: str) -> Any:
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        if result := fn():
            return result
        time.sleep(0.5)
    fail(f"timed out waiting for {what}")


def dlq_messages(client: httpx.Client) -> int:
    resp = client.get(f"{RABBIT_MGMT}/api/queues/%2F/payments.dlq", auth=("guest", "guest"))
    resp.raise_for_status()
    return int(resp.json().get("messages", 0))


def main() -> None:
    client = httpx.Client(timeout=10)
    auth = {"X-API-Key": API_KEY}
    body = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "e2e order",
        "metadata": {"order_id": "e2e-1"},
        "webhook_url": f"{SINK_INTERNAL}/hook",
    }
    key = f"e2e-{uuid.uuid4()}"
    url = f"{API}/api/v1/payments"

    resp = client.post(url, json=body, headers={"Idempotency-Key": key})
    check(resp.status_code == 401, "POST without X-API-Key -> 401")

    resp = client.post(url, json=body, headers=auth)
    check(resp.status_code == 400, "POST without Idempotency-Key -> 400")

    resp = client.post(url, json=body, headers={**auth, "Idempotency-Key": key})
    check(resp.status_code == 202, "POST -> 202")
    created = resp.json()
    payment_id = created["payment_id"]
    check(created["status"] == "pending", "new payment is pending")

    resp = client.post(url, json=body, headers={**auth, "Idempotency-Key": key})
    check(
        resp.status_code == 202 and resp.json()["payment_id"] == payment_id,
        "replay with same key -> same payment",
    )
    check(resp.headers.get("Idempotent-Replayed") == "true", "replay header present")

    resp = client.post(
        url, json={**body, "amount": "1.00"}, headers={**auth, "Idempotency-Key": key}
    )
    check(
        resp.status_code == 422 and resp.json()["detail"]["code"] == "idempotency_key_reused",
        "same key, different body -> 422 idempotency_key_reused",
    )

    resp = client.get(f"{url}/{uuid.uuid4()}", headers=auth)
    check(resp.status_code == 404, "GET unknown id -> 404")

    def processed() -> dict[str, Any] | None:
        data: dict[str, Any] = client.get(f"{url}/{payment_id}", headers=auth).json()
        return data if data["webhook_delivered_at"] else None

    payment = wait_for(processed, "payment processing + webhook delivery")
    check(payment["status"] in ("succeeded", "failed"), f"terminal status: {payment['status']}")
    hooks = [
        h
        for h in client.get(f"{SINK}/received").json()["received"]
        if h["payment_id"] == payment_id
    ]
    check(len(hooks) >= 1 and hooks[0]["status"] == payment["status"], "webhook received")

    # Retries + DLQ: the sink's /fail endpoint always answers 500.
    dlq_before = dlq_messages(client)
    resp = client.post(
        url,
        json={**body, "webhook_url": f"{SINK_INTERNAL}/fail"},
        headers={**auth, "Idempotency-Key": f"e2e-dlq-{uuid.uuid4()}"},
    )
    check(resp.status_code == 202, "POST with failing webhook -> 202")
    failing_id = resp.json()["payment_id"]

    def dead_lettered() -> dict[str, Any] | None:
        data: dict[str, Any] = client.get(f"{url}/{failing_id}", headers=auth).json()
        return data if data["webhook_attempts"] >= 3 and dlq_messages(client) > dlq_before else None

    failing = wait_for(dead_lettered, "3 webhook attempts and a DLQ message")
    check(failing["webhook_delivered_at"] is None, "failing webhook never marked delivered")
    check(failing["webhook_attempts"] == 3, "exactly 3 attempts before DLQ")
    print("E2E PASSED")


if __name__ == "__main__":
    main()
