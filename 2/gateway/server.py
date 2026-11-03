"""
Shard-aware gateway.
1) Pick owning shard from Shard Map by account range.
2) Pick a healthy replica of that shard with Round Robin.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))
SHARD_MAP_PATH = os.getenv("SHARD_MAP_PATH", "/config/shard_map.json")
HEALTH_INTERVAL = float(os.getenv("HEALTH_INTERVAL", "2"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "5"))

ACCOUNT_RE = re.compile(r"^A(\d{4})$", re.IGNORECASE)

logger = logging.getLogger("gateway")
logger.setLevel(logging.INFO)
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)


class TransactionRequest(BaseModel):
    action: str = Field(..., description="balance | deposit | withdraw")
    account: str
    amount: Optional[Any] = None


class ShardMap:
    def __init__(self, path: str) -> None:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        self.epoch = int(raw.get("epoch", 1))
        self.shards: dict[str, dict[str, Any]] = raw["shards"]
        self._rr: dict[str, int] = {sid: 0 for sid in self.shards}
        self._healthy: dict[str, bool] = {}
        for shard in self.shards.values():
            for url in shard["replicas"]:
                self._healthy[url] = False
        self._lock = threading.Lock()

    def shard_for_account(self, account: str) -> str:
        m = ACCOUNT_RE.match(account.strip())
        if not m:
            raise ValueError("invalid_account")
        num = int(m.group(1))
        for sid, meta in self.shards.items():
            if int(meta["start"]) <= num <= int(meta["end"]):
                return sid
        raise LookupError("no_shard")

    def mark(self, url: str, ok: bool) -> None:
        with self._lock:
            self._healthy[url] = ok

    def pick_replica(self, shard_id: str) -> str:
        with self._lock:
            replicas = list(self.shards[shard_id]["replicas"])
            healthy = [u for u in replicas if self._healthy.get(u)]
            if not healthy:
                raise RuntimeError("no_healthy_replica")
            idx = self._rr[shard_id] % len(healthy)
            self._rr[shard_id] += 1
            return healthy[idx]

    def status(self) -> dict[str, Any]:
        with self._lock:
            shards = {}
            for sid, meta in self.shards.items():
                shards[sid] = {
                    "start": meta["start"],
                    "end": meta["end"],
                    "replicas": [
                        {"url": u, "healthy": bool(self._healthy.get(u))}
                        for u in meta["replicas"]
                    ],
                }
            return {"epoch": self.epoch, "shards": shards}


shard_map = ShardMap(SHARD_MAP_PATH)
_stop = threading.Event()


def _health_loop() -> None:
    while not _stop.is_set():
        for meta in shard_map.shards.values():
            for url in meta["replicas"]:
                ok = False
                try:
                    r = requests.get(f"{url.rstrip('/')}/health", timeout=1.5)
                    ok = r.status_code == 200 and r.json().get("status") == "ok"
                except (requests.RequestException, ValueError, TypeError):
                    ok = False
                prev = shard_map._healthy.get(url)
                shard_map.mark(url, ok)
                if prev is True and not ok:
                    logger.info(f"replica_down  url={url}")
                elif prev is False and ok:
                    logger.info(f"replica_up  url={url}")
        _stop.wait(HEALTH_INTERVAL)


app = FastAPI(title="Shard-Aware Banking Gateway")


@app.on_event("startup")
def on_startup() -> None:
    threading.Thread(target=_health_loop, name="health-loop", daemon=True).start()
    logger.info(f"gateway_start  map={SHARD_MAP_PATH}  epoch={shard_map.epoch}")


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop.set()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "gateway", "epoch": shard_map.epoch}


@app.get("/shard-map")
def get_shard_map() -> dict:
    return shard_map.status()


@app.post("/transaction")
def transaction(body: TransactionRequest, request: Request):
    action = (body.action or "").strip().lower()
    account = (body.account or "").strip()

    if action not in {"balance", "deposit", "withdraw"}:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"unsupported action: {body.action}"},
        )
    if not account:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "missing account field"},
        )

    try:
        shard_id = shard_map.shard_for_account(account)
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"invalid account id: {account}"},
        )
    except LookupError:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"no shard owns account: {account}"},
        )

    try:
        replica = shard_map.pick_replica(shard_id)
    except RuntimeError:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "message": f"no healthy replica for shard {shard_id}",
                "shard_id": shard_id,
            },
        )

    payload = body.model_dump()
    try:
        upstream = requests.post(
            f"{replica.rstrip('/')}/transaction",
            json=payload,
            timeout=UPSTREAM_TIMEOUT,
        )
    except requests.RequestException as exc:
        shard_map.mark(replica, False)
        logger.info(f"upstream_fail  shard={shard_id}  replica={replica}  err={exc}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "message": "upstream replica unreachable",
                "shard_id": shard_id,
                "replica": replica,
            },
        )

    try:
        data = upstream.json()
    except ValueError:
        data = {"status": "error", "message": upstream.text}

    if isinstance(data, dict):
        data.setdefault("shard_id", shard_id)
        data.setdefault("gateway_replica", replica)

    client = request.client.host if request.client else "?"
    logger.info(
        f"route  ip={client}  action={action}  account={account}  "
        f"shard={shard_id}  replica={replica}  upstream_status={upstream.status_code}"
    )
    return JSONResponse(status_code=upstream.status_code, content=data)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
