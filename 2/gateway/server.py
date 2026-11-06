"""
Shard-aware gateway.
Local ops + same-shard transfer locally; cross-shard via Coordinator.
Bonus I: hot-reload shard map epochs without breaking existing clients.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import uuid
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
MAP_RELOAD_INTERVAL = float(os.getenv("MAP_RELOAD_INTERVAL", "1"))

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
    client_request_id: Optional[str] = None
    epoch: Optional[int] = None


class ShardMap:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.epoch = 1
        self.shards: dict[str, dict[str, Any]] = {}
        self._rr: dict[str, int] = {}
        self._healthy: dict[str, bool] = {}
        self._mtime: float = -1.0
        self._lock = threading.Lock()
        self.reload(force=True)

    def reload(self, force: bool = False) -> bool:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            return False
        with self._lock:
            if not force and mtime == self._mtime:
                return False
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            new_epoch = int(raw.get("epoch", 1))
            new_shards = raw["shards"]
            old_epoch = self.epoch
            self.epoch = new_epoch
            self.shards = new_shards
            self._mtime = mtime
            for sid in new_shards:
                self._rr.setdefault(sid, 0)
            # Drop RR for removed shards
            for sid in list(self._rr):
                if sid not in new_shards:
                    self._rr.pop(sid, None)
            known = set(self._healthy)
            current_urls = {u for meta in new_shards.values() for u in meta["replicas"]}
            for url in current_urls:
                self._healthy.setdefault(url, False)
            for url in list(known):
                if url not in current_urls:
                    self._healthy.pop(url, None)
            if force or new_epoch != old_epoch:
                logger.info(
                    f"shard_map_loaded  epoch={new_epoch}  shards={list(new_shards)}  "
                    f"path={self.path}"
                )
            return True

    def shard_for_account(self, account: str) -> str:
        m = ACCOUNT_RE.match(account.strip())
        if not m:
            raise ValueError("invalid_account")
        num = int(m.group(1))
        with self._lock:
            shards = dict(self.shards)
        for sid, meta in shards.items():
            if int(meta["start"]) <= num <= int(meta["end"]):
                return sid
        raise LookupError("no_shard")

    def mark(self, url: str, ok: bool) -> None:
        with self._lock:
            if url in self._healthy:
                self._healthy[url] = ok

    def pick_replica(self, shard_id: str) -> str:
        with self._lock:
            if shard_id not in self.shards:
                raise RuntimeError("unknown_shard")
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
        shard_map.reload(force=False)
        with shard_map._lock:
            urls = [u for meta in shard_map.shards.values() for u in meta["replicas"]]
        for url in urls:
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
        data.setdefault("map_epoch", shard_map.epoch)
    return JSONResponse(status_code=upstream.status_code, content=data)


def _enrich_payload(body: TransactionRequest) -> dict:
    payload = body.model_dump()
    payload["epoch"] = body.epoch if body.epoch is not None else shard_map.epoch
    if not payload.get("client_request_id"):
        # Always attach an id so retries/duplicates are safe during Bonus I demos.
        payload["client_request_id"] = uuid.uuid4().hex
    return payload


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
    shard_map.reload(force=False)
    return {"status": "ok", "service": "gateway", "epoch": shard_map.epoch}


@app.get("/shard-map")
def get_shard_map() -> dict:
    shard_map.reload(force=False)
    return shard_map.status()


@app.post("/admin/reload-map")
def reload_map() -> dict:
    changed = shard_map.reload(force=True)
    return {"status": "ok", "reloaded": changed, "epoch": shard_map.epoch, "map": shard_map.status()}


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


@app.get("/requests/{client_request_id}")
def proxy_request_status(client_request_id: str):
    """Uncertain-outcome helper: ask coordinator first, else unknown."""
    try:
        r = requests.get(
            f"{COORDINATOR_URL.rstrip('/')}/requests/{client_request_id}",
            timeout=UPSTREAM_TIMEOUT,
        )
        if r.status_code != 404:
            return JSONResponse(status_code=r.status_code, content=r.json())
    except requests.RequestException:
        pass
    return {
        "status": "UNKNOWN",
        "client_request_id": client_request_id,
        "message": "no durable client request found yet; retry safely with same id",
    }


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

    shard_map.reload(force=False)
    client = request.client.host if request.client else "?"
    payload = _enrich_payload(body)

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
                f"shard={from_shard}  replica={replica}  epoch={payload['epoch']}"
            )
            return _forward(replica, payload)

        logger.info(
            f"route_cross_shard_transfer  ip={client}  from={account}  to={to_account}  "
            f"from_shard={from_shard}  to_shard={to_shard}  epoch={payload['epoch']}"
        )
        try:
            r = requests.post(
                f"{COORDINATOR_URL.rstrip('/')}/transfer",
                json={
                    "from_account": account,
                    "to_account": to_account,
                    "amount": body.amount,
                    "client_request_id": payload["client_request_id"],
                    "epoch": payload["epoch"],
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
        f"shard={shard_id}  replica={replica}  epoch={payload['epoch']}"
    )
    return _forward(replica, payload)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
