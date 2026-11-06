"""
Shard bank application replica.
Local ops + same-shard transfer + 2PC participant endpoints.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import sql
from psycopg.rows import dict_row
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

import shard_support

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://bank:bank@127.0.0.1:5432/bank",
)
REPLICA_ID = os.getenv("REPLICA_ID", "unknown")
SHARD_ID = os.getenv("SHARD_ID", "unknown")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", "50"))
# Bonus I defaults; migrator may shrink/expand ownership at runtime.
OWNED_START = int(os.getenv("OWNED_START", "1" if SHARD_ID == "A" else "1001" if SHARD_ID == "B" else "1501"))
OWNED_END = int(os.getenv("OWNED_END", "1000" if SHARD_ID == "A" else "2000" if SHARD_ID == "B" else "2000"))
WRITES_ENABLED = os.getenv("WRITES_ENABLED", "true" if SHARD_ID != "C" else "false").lower() in {
    "1",
    "true",
    "yes",
}

_client_slots = __import__("threading").Semaphore(MAX_CLIENTS)

logger = logging.getLogger("bank")
logger.setLevel(logging.INFO)
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def get_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


@contextmanager
def db_transaction():
    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                yield cur


class TransactionRequest(BaseModel):
    action: str = Field(..., description="balance | deposit | withdraw | transfer")
    account: str = Field(..., description="Account ID, or source for transfer")
    amount: Optional[Any] = Field(default=None)
    to_account: Optional[str] = Field(default=None)
    client_request_id: Optional[str] = Field(default=None)
    epoch: Optional[int] = Field(default=None)


class PrepareRequest(BaseModel):
    transaction_id: str
    operation: str  # debit | credit
    account: str
    amount: Any
    epoch: Optional[int] = None


class DecisionRequest(BaseModel):
    transaction_id: str


class OwnershipBody(BaseModel):
    epoch: Optional[int] = None
    owned_start: Optional[int] = None
    owned_end: Optional[int] = None
    write_enabled: Optional[bool] = None
    fenced_start: Optional[int] = None
    fenced_end: Optional[int] = None
    clear_fence: bool = False


class SnapshotBody(BaseModel):
    start: int
    end: int


class ApplySnapshotBody(BaseModel):
    accounts: list[dict[str, Any]]


class ApplyChangesBody(BaseModel):
    changes: list[dict[str, Any]]


class RangeBody(BaseModel):
    start: int
    end: int
    after_seq: int = 0
    limit: int = 500


app = FastAPI(title="Distributed Bank Shard Replica")


@app.on_event("startup")
def on_startup() -> None:
    with get_connection() as conn:
        shard_support.ensure_schema(
            conn, shard_id=SHARD_ID, default_start=OWNED_START, default_end=OWNED_END
        )
        with conn.cursor() as cur:
            # Keep env defaults only when row still matches bootstrap; migrator owns later updates.
            own = shard_support.get_ownership(cur)
            if own["owned_start"] == OWNED_START and own["owned_end"] == OWNED_END:
                shard_support.set_ownership(cur, write_enabled=WRITES_ENABLED)
            conn.commit()
            cur.execute("SELECT 1")
    logger.info(
        f"{utc_now()}  server_start  replica={REPLICA_ID}  shard={SHARD_ID}  "
        f"owned={OWNED_START}-{OWNED_END}  writes={WRITES_ENABLED}  "
        f"listening={HOST}:{PORT}"
    )


@app.get("/health")
def health() -> dict:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                own = shard_support.get_ownership(cur)
        return {
            "status": "ok",
            "replica_id": REPLICA_ID,
            "shard_id": SHARD_ID,
            "epoch": int(own["epoch"]),
            "owned_start": int(own["owned_start"]),
            "owned_end": int(own["owned_end"]),
            "write_enabled": bool(own["write_enabled"]),
        }
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "replica_id": REPLICA_ID,
                "shard_id": SHARD_ID,
                "message": str(exc),
            },
        )


def _fence_error(code: str, account: Optional[str] = None) -> JSONResponse:
    messages = {
        "stale_owner": "stale route rejected: this shard is not the owner",
        "stale_epoch": "stale epoch rejected: shard map epoch moved forward",
        "writes_disabled": "writes temporarily disabled on this shard",
        "account_fenced": "account range fenced for live re-sharding cutover",
        "invalid_account": "invalid account id",
    }
    return _error("error", messages.get(code, code), 409, account)


@app.get("/admin/ownership")
def admin_ownership():
    with get_connection() as conn:
        with conn.cursor() as cur:
            return shard_support.get_ownership(cur)


@app.post("/admin/ownership")
def admin_set_ownership(body: OwnershipBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            own = shard_support.set_ownership(
                cur,
                epoch=body.epoch,
                owned_start=body.owned_start,
                owned_end=body.owned_end,
                write_enabled=body.write_enabled,
                fenced_start=body.fenced_start,
                fenced_end=body.fenced_end,
                clear_fence=body.clear_fence,
            )
        conn.commit()
    logger.info(
        f"{utc_now()}  ownership_update  shard={SHARD_ID}  replica={REPLICA_ID}  "
        f"epoch={own['epoch']}  owned={own['owned_start']}-{own['owned_end']}  "
        f"writes={own['write_enabled']}  fence={own['fenced_start']}-{own['fenced_end']}"
    )
    return own


@app.post("/admin/snapshot")
def admin_snapshot(body: SnapshotBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            rows, pos = shard_support.snapshot_accounts(cur, body.start, body.end)
            total = shard_support.sum_range(cur, body.start, body.end)
        conn.commit()
    return {
        "start": body.start,
        "end": body.end,
        "count": len(rows),
        "change_pos": pos,
        "sum_balance": float(total),
        "accounts": rows,
    }


@app.post("/admin/changes")
def admin_changes(body: RangeBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            changes = shard_support.fetch_changes(
                cur, body.after_seq, body.start, body.end, body.limit
            )
            max_seq = shard_support.changelog_max_seq(cur)
        conn.commit()
    return {
        "after_seq": body.after_seq,
        "max_seq": max_seq,
        "count": len(changes),
        "changes": changes,
    }


@app.post("/admin/apply_snapshot")
def admin_apply_snapshot(body: ApplySnapshotBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            n = shard_support.apply_snapshot_batch(cur, body.accounts)
        conn.commit()
    return {"status": "ok", "applied": n, "replica_id": REPLICA_ID, "shard_id": SHARD_ID}


@app.post("/admin/apply_changes")
def admin_apply_changes(body: ApplyChangesBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            n = shard_support.apply_changes_idempotent(cur, body.changes)
            max_applied = 0
            if body.changes:
                max_applied = max(int(c["seq"]) for c in body.changes)
        conn.commit()
    return {
        "status": "ok",
        "applied": n,
        "max_seq": max_applied,
        "replica_id": REPLICA_ID,
        "shard_id": SHARD_ID,
    }


@app.post("/admin/delete_range")
def admin_delete_range(body: SnapshotBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            n = shard_support.delete_account_range(cur, body.start, body.end)
        conn.commit()
    logger.info(
        f"{utc_now()}  cleanup_range  shard={SHARD_ID}  start={body.start}  "
        f"end={body.end}  deleted={n}"
    )
    return {"status": "ok", "deleted": n}


@app.post("/admin/range_stats")
def admin_range_stats(body: SnapshotBody):
    with get_connection() as conn:
        with conn.cursor() as cur:
            return {
                "start": body.start,
                "end": body.end,
                "count": shard_support.count_range(cur, body.start, body.end),
                "sum_balance": float(shard_support.sum_range(cur, body.start, body.end)),
                "change_pos": shard_support.changelog_max_seq(cur),
                "ownership": shard_support.get_ownership(cur),
            }


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _parse_amount(raw: Any) -> Decimal:
    if raw is None:
        raise ValueError("missing_amount")
    try:
        amount = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("invalid_amount") from None
    if amount <= 0:
        raise ValueError("invalid_amount")
    return amount.quantize(Decimal("0.01"))


def _error(
    status: str, message: str, http_code: int, account: Optional[str] = None
) -> JSONResponse:
    body: dict[str, Any] = {
        "status": status,
        "message": message,
        "replica_id": REPLICA_ID,
        "shard_id": SHARD_ID,
    }
    if account is not None:
        body["account"] = account
    return JSONResponse(status_code=http_code, content=body)


def _prepared_gid(transaction_id: str) -> str:
    # PostgreSQL gid max 200 chars; keep stable per shard participant.
    return f"{transaction_id}-{SHARD_ID}"


def _is_prepared(cur, gid: str) -> bool:
    cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
    return cur.fetchone() is not None


@app.post("/transaction")
def transaction(body: TransactionRequest, request: Request):
    started = time.perf_counter()
    request_id = uuid.uuid4().hex[:8]
    client_ip = _client_ip(request)
    action = (body.action or "").strip().lower()
    account = (body.account or "").strip()
    to_account = (body.to_account or "").strip() if body.to_account else ""

    if not _client_slots.acquire(blocking=False):
        return _error("error", "server busy: max concurrent clients reached", 503, account or None)

    try:
        if not account:
            return _error("error", "missing account field", 400)
        if action not in {"balance", "deposit", "withdraw", "transfer"}:
            return _error("error", f"unsupported action: {body.action}", 400, account)
        if action == "transfer" and not to_account:
            return _error("error", "missing to_account field", 400, account)

        client_request_id = (body.client_request_id or "").strip() or None
        req_epoch = body.epoch

        try:
            with db_transaction() as cur:
                if client_request_id:
                    cached = shard_support.get_cached_client_request(cur, client_request_id)
                    if cached is not None:
                        cached = dict(cached)
                        cached["idempotent_replay"] = True
                        cached["replica_id"] = REPLICA_ID
                        return cached

                # Reads allowed on owned accounts even during brief fence; writes are fenced.
                if action != "balance":
                    for acc in ([account, to_account] if action == "transfer" else [account]):
                        if not acc:
                            continue
                        denied = shard_support.check_write_allowed(
                            cur, acc, request_epoch=req_epoch
                        )
                        if denied:
                            raise RuntimeError(f"fence:{denied}")

                if action == "balance":
                    # Stale owner still rejected so demos can prove fencing of routes.
                    denied = shard_support.check_write_allowed(
                        cur, account, request_epoch=req_epoch, allow_during_fence=True
                    )
                    if denied in {"stale_owner", "stale_epoch", "invalid_account"}:
                        raise RuntimeError(f"fence:{denied}")
                    cur.execute(
                        "SELECT account_id, balance FROM accounts WHERE account_id = %s",
                        (account,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise LookupError("unknown_account")
                    result_balance = Decimal(row["balance"])
                    response = {
                        "status": "success",
                        "account": account,
                        "balance": float(result_balance),
                        "request_id": request_id,
                        "client_request_id": client_request_id,
                        "replica_id": REPLICA_ID,
                        "shard_id": SHARD_ID,
                        "epoch": int(shard_support.get_ownership(cur)["epoch"]),
                    }
                elif action == "transfer":
                    amount = _parse_amount(body.amount)
                    # Lock in stable order to avoid deadlocks between concurrent transfers.
                    first, second = sorted([account, to_account])
                    cur.execute(
                        "SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE",
                        (first,),
                    )
                    row1 = cur.fetchone()
                    cur.execute(
                        "SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE",
                        (second,),
                    )
                    row2 = cur.fetchone()
                    if row1 is None or row2 is None:
                        raise LookupError("unknown_account")

                    balances = {
                        row1["account_id"]: Decimal(row1["balance"]),
                        row2["account_id"]: Decimal(row2["balance"]),
                    }
                    if balances[account] < amount:
                        raise RuntimeError("insufficient_funds")
                    balances[account] -= amount
                    balances[to_account] += amount
                    cur.execute(
                        "UPDATE accounts SET balance = %s WHERE account_id = %s",
                        (balances[account], account),
                    )
                    cur.execute(
                        "UPDATE accounts SET balance = %s WHERE account_id = %s",
                        (balances[to_account], to_account),
                    )
                    response = {
                        "status": "success",
                        "action": "transfer",
                        "from_account": account,
                        "to_account": to_account,
                        "amount": float(amount),
                        "from_balance": float(balances[account]),
                        "to_balance": float(balances[to_account]),
                        "request_id": request_id,
                        "client_request_id": client_request_id,
                        "replica_id": REPLICA_ID,
                        "shard_id": SHARD_ID,
                        "mode": "same_shard",
                        "epoch": int(shard_support.get_ownership(cur)["epoch"]),
                    }
                else:
                    cur.execute(
                        "SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE",
                        (account,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise LookupError("unknown_account")

                    balance = Decimal(row["balance"])
                    amount = _parse_amount(body.amount)

                    if action == "deposit":
                        balance = balance + amount
                    else:
                        if balance < amount:
                            raise RuntimeError("insufficient_funds")
                        balance = balance - amount

                    cur.execute(
                        "UPDATE accounts SET balance = %s WHERE account_id = %s",
                        (balance, account),
                    )
                    response = {
                        "status": "success",
                        "account": account,
                        "balance": float(balance),
                        "request_id": request_id,
                        "client_request_id": client_request_id,
                        "replica_id": REPLICA_ID,
                        "shard_id": SHARD_ID,
                        "epoch": int(shard_support.get_ownership(cur)["epoch"]),
                    }

                if client_request_id:
                    amt = None
                    if action != "balance":
                        amt = _parse_amount(body.amount) if body.amount is not None else None
                    shard_support.store_client_request(
                        cur,
                        client_request_id,
                        action,
                        account,
                        to_account or None,
                        amt if action != "balance" else None,
                        response,
                    )

            latency = (time.perf_counter() - started) * 1000
            logger.info(
                f"{utc_now()}  req={request_id}  ip={client_ip}  "
                f"replica={REPLICA_ID}  shard={SHARD_ID}  "
                f"action={action}  account={account}  result=success  "
                f"latency={latency:.0f}ms"
            )
            return response

        except LookupError:
            missing = account
            if action == "transfer":
                missing = f"{account} or {to_account}"
            return _error("error", f"unknown account: {missing}", 404, account)
        except ValueError as exc:
            messages = {
                "missing_amount": "missing amount field",
                "invalid_amount": "amount must be a positive number",
            }
            return _error("error", messages.get(str(exc), "invalid request"), 400, account)
        except RuntimeError as exc:
            msg = str(exc)
            if msg == "insufficient_funds":
                return _error("error", "insufficient funds", 400, account)
            if msg.startswith("fence:"):
                return _fence_error(msg.split(":", 1)[1], account)
            raise
        except Exception:
            logger.exception("transaction failed")
            return _error(
                "error", "internal server error; no balance change applied", 500, account
            )
    finally:
        _client_slots.release()


@app.post("/2pc/prepare")
def prepare(body: PrepareRequest):
    op = (body.operation or "").strip().lower()
    account = (body.account or "").strip()
    if op not in {"debit", "credit"}:
        return _error("error", "operation must be debit or credit", 400, account or None)
    if not account or not body.transaction_id:
        return _error("error", "missing transaction_id or account", 400, account or None)

    gid = _prepared_gid(body.transaction_id)
    try:
        amount = _parse_amount(body.amount)
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"status": "vote_no", "vote": "NO", "reason": str(exc), "gid": gid},
        )

    conn = get_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            denied = shard_support.check_write_allowed(
                cur, account, request_epoch=body.epoch
            )
            if denied:
                conn.rollback()
                return JSONResponse(
                    status_code=409,
                    content={
                        "status": "vote_no",
                        "vote": "NO",
                        "reason": denied,
                        "gid": gid,
                        "shard_id": SHARD_ID,
                    },
                )

            if _is_prepared(cur, gid):
                # Idempotent retry of PREPARE.
                return {
                    "status": "vote_yes",
                    "vote": "YES",
                    "gid": gid,
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                }

            cur.execute(
                "SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE",
                (account,),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return JSONResponse(
                    status_code=404,
                    content={
                        "status": "vote_no",
                        "vote": "NO",
                        "reason": f"unknown account: {account}",
                        "gid": gid,
                    },
                )

            balance = Decimal(row["balance"])
            if op == "debit":
                if balance < amount:
                    conn.rollback()
                    return JSONResponse(
                        status_code=400,
                        content={
                            "status": "vote_no",
                            "vote": "NO",
                            "reason": "insufficient funds",
                            "gid": gid,
                        },
                    )
                balance -= amount
            else:
                balance += amount

            cur.execute(
                "UPDATE accounts SET balance = %s WHERE account_id = %s",
                (balance, account),
            )
            # PREPARE TRANSACTION takes a string literal, not a bind parameter.
            cur.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
        # PREPARE TRANSACTION ends the tx; do not commit afterwards.
        logger.info(
            f"{utc_now()}  2pc_prepare  gid={gid}  op={op}  account={account}  vote=YES  "
            f"replica={REPLICA_ID}"
        )
        return {
            "status": "vote_yes",
            "vote": "YES",
            "gid": gid,
            "shard_id": SHARD_ID,
            "replica_id": REPLICA_ID,
            "balance": float(balance),
        }
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("prepare failed")
        return JSONResponse(
            status_code=500,
            content={"status": "vote_no", "vote": "NO", "reason": str(exc), "gid": gid},
        )
    finally:
        conn.close()


@app.post("/2pc/commit")
def commit_prepared(body: DecisionRequest):
    gid = _prepared_gid(body.transaction_id)
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            if not _is_prepared(cur, gid):
                # Already committed (or never prepared) — safe retry.
                return {
                    "status": "committed",
                    "gid": gid,
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                    "idempotent": True,
                }
            cur.execute(sql.SQL("COMMIT PREPARED {}").format(sql.Literal(gid)))
    logger.info(f"{utc_now()}  2pc_commit  gid={gid}  replica={REPLICA_ID}")
    return {
        "status": "committed",
        "gid": gid,
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
    }


@app.post("/2pc/abort")
def abort_prepared(body: DecisionRequest):
    gid = _prepared_gid(body.transaction_id)
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            if not _is_prepared(cur, gid):
                return {
                    "status": "aborted",
                    "gid": gid,
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                    "idempotent": True,
                }
            cur.execute(sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid)))
    logger.info(f"{utc_now()}  2pc_abort  gid={gid}  replica={REPLICA_ID}")
    return {
        "status": "aborted",
        "gid": gid,
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
    }


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content={"status": "error", "message": "invalid or missing fields", "details": exc.errors()},
    )


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": str(exc.detail)},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
