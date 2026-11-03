"""
Shard-aware gateway.
Local ops + same-shard transfer locally; cross-shard via Coordinator.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from pathlib import Path
from typing import Any, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))
SHARD_MAP_PATH = os.getenv("SHARD_MAP_PATH", "/config/shard_map.json")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8090")
HEALTH_INTERVAL = float(os.getenv("HEALTH_INTERVAL", "2"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "10"))

ACCOUNT_RE = re.compile(r"^A(\d{4})$", re.IGNORECASE)

logger = logging.getLogger("gateway")
logger.setLevel(logging.INFO)
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)


class TransactionRequest(BaseModel):
    action: str = Field(..., description="balance | deposit | withdraw | transfer")
    account: str
    amount: Optional[Any] = None
    to_account: Optional[str] = None


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


def _forward(replica: str, payload: dict) -> JSONResponse:
    try:
        upstream = requests.post(
            f"{replica.rstrip('/')}/transaction",
            json=payload,
            timeout=UPSTREAM_TIMEOUT,
        )
    except requests.RequestException as exc:
        shard_map.mark(replica, False)
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "message": "upstream replica unreachable",
                "replica": replica,
                "detail": str(exc),
            },
        )
    try:
        data = upstream.json()
    except ValueError:
        data = {"status": "error", "message": upstream.text}
    if isinstance(data, dict):
        data.setdefault("gateway_replica", replica)
    return JSONResponse(status_code=upstream.status_code, content=data)


app = FastAPI(title="Shard-Aware Banking Gateway")


@app.on_event("startup")
def on_startup() -> None:
    threading.Thread(target=_health_loop, name="health-loop", daemon=True).start()
    logger.info(
        f"gateway_start  map={SHARD_MAP_PATH}  epoch={shard_map.epoch}  "
        f"coordinator={COORDINATOR_URL}"
    )


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop.set()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "gateway", "epoch": shard_map.epoch}


@app.get("/shard-map")
def get_shard_map() -> dict:
    return shard_map.status()


@app.get("/transactions/{tx_id}")
def proxy_tx_status(tx_id: str):
    try:
        r = requests.get(
            f"{COORDINATOR_URL.rstrip('/')}/transactions/{tx_id}",
            timeout=UPSTREAM_TIMEOUT,
        )
        return JSONResponse(status_code=r.status_code, content=r.json())
    except (requests.RequestException, ValueError) as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": f"coordinator unreachable: {exc}"},
        )


@app.post("/transaction")
def transaction(body: TransactionRequest, request: Request):
    action = (body.action or "").strip().lower()
    account = (body.account or "").strip()
    to_account = (body.to_account or "").strip() if body.to_account else ""

    if action not in {"balance", "deposit", "withdraw", "transfer"}:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"unsupported action: {body.action}"},
        )
    if not account:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "missing account field"},
        )

    client = request.client.host if request.client else "?"

    if action == "transfer":
        if not to_account:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "missing to_account field"},
            )
        try:
            from_shard = shard_map.shard_for_account(account)
            to_shard = shard_map.shard_for_account(to_account)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "invalid account id"},
            )
        except LookupError:
            return JSONResponse(
                status_code=404,
                content={"status": "error", "message": "no shard owns one of the accounts"},
            )

        if from_shard == to_shard:
            try:
                replica = shard_map.pick_replica(from_shard)
            except RuntimeError:
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "error",
                        "message": f"no healthy replica for shard {from_shard}",
                    },
                )
            logger.info(
                f"route_same_shard_transfer  ip={client}  from={account}  to={to_account}  "
                f"shard={from_shard}  replica={replica}"
            )
            return _forward(replica, body.model_dump())

        # Cross-shard → Coordinator (2PC)
        logger.info(
            f"route_cross_shard_transfer  ip={client}  from={account}  to={to_account}  "
            f"from_shard={from_shard}  to_shard={to_shard}"
        )
        try:
            r = requests.post(
                f"{COORDINATOR_URL.rstrip('/')}/transfer",
                json={
                    "from_account": account,
                    "to_account": to_account,
                    "amount": body.amount,
                },
                timeout=UPSTREAM_TIMEOUT,
            )
            try:
                data = r.json()
            except ValueError:
                data = {"status": "error", "message": r.text}
            return JSONResponse(status_code=r.status_code, content=data)
        except requests.RequestException as exc:
            return JSONResponse(
                status_code=503,
                content={"status": "error", "message": f"coordinator unreachable: {exc}"},
            )

    # Local single-account ops
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

    logger.info(
        f"route  ip={client}  action={action}  account={account}  "
        f"shard={shard_id}  replica={replica}"
    )
    resp = _forward(replica, body.model_dump())
    return resp


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
