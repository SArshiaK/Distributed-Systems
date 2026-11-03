"""
Transaction Coordinator for cross-shard transfers (2PC).
Durable decisions in PostgreSQL; recovers unresolved txs after restart.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import psycopg
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://bank:bank@127.0.0.1:5433/coord",
)
SHARD_MAP_PATH = os.getenv("SHARD_MAP_PATH", "/config/shard_map.json")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8090"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "5"))
RECOVERY_INTERVAL = float(os.getenv("RECOVERY_INTERVAL", "3"))

ACCOUNT_RE = re.compile(r"^A(\d{4})$", re.IGNORECASE)
TERMINAL = {"COMPLETED"}
NEEDS_RECOVERY = {
    "INITIATED",
    "PREPARING",
    "PREPARED",
    "COMMIT_DECIDED",
    "ABORT_DECIDED",
}

logger = logging.getLogger("coordinator")
logger.setLevel(logging.INFO)
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)

_stop = threading.Event()
_rr: dict[str, int] = {}
_healthy: dict[str, bool] = {}
_lock = threading.Lock()
_shards: dict[str, dict[str, Any]] = {}


class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: Any


def get_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def load_shard_map() -> None:
    global _shards
    raw = json.loads(Path(SHARD_MAP_PATH).read_text(encoding="utf-8"))
    _shards = raw["shards"]
    with _lock:
        for sid, meta in _shards.items():
            _rr.setdefault(sid, 0)
            for url in meta["replicas"]:
                _healthy.setdefault(url, False)


def shard_for_account(account: str) -> str:
    m = ACCOUNT_RE.match(account.strip())
    if not m:
        raise ValueError("invalid_account")
    num = int(m.group(1))
    for sid, meta in _shards.items():
        if int(meta["start"]) <= num <= int(meta["end"]):
            return sid
    raise LookupError("no_shard")


def mark(url: str, ok: bool) -> None:
    with _lock:
        _healthy[url] = ok


def pick_replica(shard_id: str) -> str:
    with _lock:
        replicas = list(_shards[shard_id]["replicas"])
        healthy = [u for u in replicas if _healthy.get(u)]
        if not healthy:
            raise RuntimeError(f"no healthy replica for shard {shard_id}")
        idx = _rr[shard_id] % len(healthy)
        _rr[shard_id] += 1
        return healthy[idx]


def parse_amount(raw: Any) -> Decimal:
    try:
        amount = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("invalid_amount") from None
    if amount <= 0:
        raise ValueError("invalid_amount")
    return amount.quantize(Decimal("0.01"))


def set_status(tx_id: str, status: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE distributed_transactions "
                "SET status = %s, updated_at = NOW() WHERE transaction_id = %s",
                (status, tx_id),
            )
        conn.commit()


def set_vote(tx_id: str, shard_id: str, vote: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transaction_participants SET vote = %s "
                "WHERE transaction_id = %s AND shard_id = %s",
                (vote, tx_id, shard_id),
            )
        conn.commit()


def set_ack(tx_id: str, shard_id: str, ack: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transaction_participants SET ack = %s "
                "WHERE transaction_id = %s AND shard_id = %s",
                (ack, tx_id, shard_id),
            )
        conn.commit()


def get_tx(tx_id: str) -> Optional[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM distributed_transactions WHERE transaction_id = %s",
                (tx_id,),
            )
            return cur.fetchone()


def get_participants(tx_id: str) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM transaction_participants WHERE transaction_id = %s "
                "ORDER BY shard_id",
                (tx_id,),
            )
            return list(cur.fetchall())


def create_tx(
    tx_id: str,
    from_account: str,
    to_account: str,
    amount: Decimal,
    from_shard: str,
    to_shard: str,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO distributed_transactions "
                "(transaction_id, from_account, to_account, amount, from_shard, to_shard, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    tx_id,
                    from_account,
                    to_account,
                    amount,
                    from_shard,
                    to_shard,
                    "INITIATED",
                ),
            )
            cur.execute(
                "INSERT INTO transaction_participants "
                "(transaction_id, shard_id, operation, account) VALUES (%s, %s, %s, %s)",
                (tx_id, from_shard, "debit", from_account),
            )
            cur.execute(
                "INSERT INTO transaction_participants "
                "(transaction_id, shard_id, operation, account) VALUES (%s, %s, %s, %s)",
                (tx_id, to_shard, "credit", to_account),
            )
        conn.commit()


def call_participant(shard_id: str, path: str, payload: dict) -> dict:
    replica = pick_replica(shard_id)
    try:
        r = requests.post(
            f"{replica.rstrip('/')}{path}",
            json=payload,
            timeout=UPSTREAM_TIMEOUT,
        )
    except requests.RequestException as exc:
        mark(replica, False)
        raise RuntimeError(f"participant unreachable: {replica}: {exc}") from exc
    try:
        data = r.json()
    except ValueError:
        data = {"status": "error", "message": r.text}
    data["_http_status"] = r.status_code
    data["_replica"] = replica
    return data


def run_prepare(tx: dict) -> bool:
    tx_id = tx["transaction_id"]
    set_status(tx_id, "PREPARING")
    amount = Decimal(tx["amount"])
    all_yes = True
    for p in get_participants(tx_id):
        try:
            resp = call_participant(
                p["shard_id"],
                "/2pc/prepare",
                {
                    "transaction_id": tx_id,
                    "operation": p["operation"],
                    "account": p["account"],
                    "amount": str(amount),
                },
            )
            vote = str(resp.get("vote", "NO")).upper()
        except RuntimeError as exc:
            logger.info(f"prepare_fail  tx={tx_id}  shard={p['shard_id']}  err={exc}")
            vote = "NO"
            resp = {"reason": str(exc)}
        set_vote(tx_id, p["shard_id"], vote)
        if vote != "YES":
            all_yes = False
            logger.info(
                f"vote_no  tx={tx_id}  shard={p['shard_id']}  reason={resp.get('reason')}"
            )
    if all_yes:
        set_status(tx_id, "PREPARED")
        # Decision must be durable BEFORE sending COMMIT.
        set_status(tx_id, "COMMIT_DECIDED")
        return True
    set_status(tx_id, "ABORT_DECIDED")
    return False


def run_commit(tx_id: str) -> bool:
    ok_all = True
    for p in get_participants(tx_id):
        try:
            call_participant(
                p["shard_id"],
                "/2pc/commit",
                {"transaction_id": tx_id},
            )
            set_ack(tx_id, p["shard_id"], "COMMIT_ACK")
        except RuntimeError as exc:
            ok_all = False
            logger.info(f"commit_ack_fail  tx={tx_id}  shard={p['shard_id']}  err={exc}")
    if ok_all:
        set_status(tx_id, "COMPLETED")
    return ok_all


def run_abort(tx_id: str) -> bool:
    ok_all = True
    for p in get_participants(tx_id):
        try:
            call_participant(
                p["shard_id"],
                "/2pc/abort",
                {"transaction_id": tx_id},
            )
            set_ack(tx_id, p["shard_id"], "ABORT_ACK")
        except RuntimeError as exc:
            ok_all = False
            logger.info(f"abort_ack_fail  tx={tx_id}  shard={p['shard_id']}  err={exc}")
    if ok_all:
        set_status(tx_id, "COMPLETED")
    return ok_all


def recover_one(tx: dict) -> None:
    tx_id = tx["transaction_id"]
    status = tx["status"]
    logger.info(f"recover  tx={tx_id}  status={status}")
    if status in {"INITIATED", "PREPARING"}:
        # Not all prepared safely → abort path.
        set_status(tx_id, "ABORT_DECIDED")
        run_abort(tx_id)
    elif status == "PREPARED":
        # Ambiguous before decision: safest is abort only if decision not recorded.
        # Assignment: once COMMIT recorded, never flip to ABORT. PREPARED has no decision yet.
        set_status(tx_id, "ABORT_DECIDED")
        run_abort(tx_id)
    elif status == "COMMIT_DECIDED":
        run_commit(tx_id)
    elif status == "ABORT_DECIDED":
        run_abort(tx_id)


def recover_all() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM distributed_transactions WHERE status = ANY(%s)",
                (list(NEEDS_RECOVERY),),
            )
            rows = list(cur.fetchall())
    for tx in rows:
        try:
            recover_one(tx)
        except Exception:
            logger.exception(f"recover_error  tx={tx['transaction_id']}")


def health_loop() -> None:
    while not _stop.is_set():
        for meta in _shards.values():
            for url in meta["replicas"]:
                ok = False
                try:
                    r = requests.get(f"{url.rstrip('/')}/health", timeout=1.5)
                    ok = r.status_code == 200 and r.json().get("status") == "ok"
                except (requests.RequestException, ValueError, TypeError):
                    ok = False
                mark(url, ok)
        _stop.wait(2)


def recovery_loop() -> None:
    while not _stop.is_set():
        try:
            recover_all()
        except Exception:
            logger.exception("recovery_loop_error")
        _stop.wait(RECOVERY_INTERVAL)


app = FastAPI(title="Transaction Coordinator")


@app.on_event("startup")
def on_startup() -> None:
    load_shard_map()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    threading.Thread(target=health_loop, name="coord-health", daemon=True).start()
    recover_all()
    threading.Thread(target=recovery_loop, name="coord-recovery", daemon=True).start()
    logger.info(f"coordinator_start  port={PORT}")


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop.set()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "coordinator"}


@app.get("/transactions/{tx_id}")
def transaction_status(tx_id: str):
    tx = get_tx(tx_id)
    if tx is None:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "unknown transaction"},
        )
    participants = get_participants(tx_id)
    state = tx["status"]
    client_status = state
    if state not in TERMINAL:
        client_status = "PENDING" if state != "INITIATED" else "UNKNOWN"
    return {
        "transaction_id": tx_id,
        "status": state,
        "client_status": client_status,
        "from_account": tx["from_account"],
        "to_account": tx["to_account"],
        "amount": float(tx["amount"]),
        "from_shard": tx["from_shard"],
        "to_shard": tx["to_shard"],
        "participants": participants,
    }


@app.post("/transfer")
def transfer(body: TransferRequest):
    from_account = body.from_account.strip()
    to_account = body.to_account.strip()
    if from_account == to_account:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "from_account and to_account must differ"},
        )
    try:
        amount = parse_amount(body.amount)
        from_shard = shard_for_account(from_account)
        to_shard = shard_for_account(to_account)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(exc)})
    except LookupError:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "account has no owning shard"},
        )

    if from_shard == to_shard:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": "same-shard transfer must go through gateway local path",
            },
        )

    tx_id = uuid.uuid4().hex
    create_tx(tx_id, from_account, to_account, amount, from_shard, to_shard)
    tx = get_tx(tx_id)
    assert tx is not None

    try:
        commit = run_prepare(tx)
        if commit:
            done = run_commit(tx_id)
            if not done:
                return JSONResponse(
                    status_code=202,
                    content={
                        "status": "PENDING",
                        "message": "COMMIT decided; waiting for participant acks",
                        "transaction_id": tx_id,
                        "mode": "cross_shard",
                    },
                )
            return {
                "status": "success",
                "transaction_id": tx_id,
                "from_account": from_account,
                "to_account": to_account,
                "amount": float(amount),
                "mode": "cross_shard",
                "decision": "COMMIT",
            }
        done = run_abort(tx_id)
        return JSONResponse(
            status_code=400,
            content={
                "status": "aborted",
                "transaction_id": tx_id,
                "mode": "cross_shard",
                "decision": "ABORT",
                "message": "2PC aborted",
                "completed": done,
            },
        )
    except Exception as exc:
        logger.exception(f"transfer_error  tx={tx_id}")
        # Fail closed toward abort if decision not yet commit.
        current = get_tx(tx_id)
        if current and current["status"] == "COMMIT_DECIDED":
            return JSONResponse(
                status_code=202,
                content={
                    "status": "PENDING",
                    "transaction_id": tx_id,
                    "message": str(exc),
                },
            )
        set_status(tx_id, "ABORT_DECIDED")
        run_abort(tx_id)
        return JSONResponse(
            status_code=500,
            content={
                "status": "aborted",
                "transaction_id": tx_id,
                "message": str(exc),
            },
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
