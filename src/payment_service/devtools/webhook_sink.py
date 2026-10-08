"""Dev-only webhook receiver used by the e2e smoke test and the README demo.

POST /hook      -> 200, records the JSON body
POST /fail      -> 500 (always), records the attempt — drives a payment into the DLQ
GET  /received  -> everything recorded so far
"""

from typing import Any

from fastapi import FastAPI, Request, Response

app = FastAPI(title="webhook-sink")
_received: list[dict[str, Any]] = []
_failed: list[dict[str, Any]] = []


@app.post("/hook")
async def hook(request: Request) -> dict[str, str]:
    _received.append(await request.json())
    return {"status": "ok"}


@app.post("/fail")
async def fail(request: Request) -> Response:
    _failed.append(await request.json())
    return Response(status_code=500)


@app.get("/received")
async def received() -> dict[str, list[dict[str, Any]]]:
    return {"received": _received, "failed": _failed}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
