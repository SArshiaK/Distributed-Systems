"""
Shard bank application replica.
All replicas of one shard share that shard's PostgreSQL database.
Replicas hold no independent authoritative balances.
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
from psycopg.rows import dict_row
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://bank:bank@127.0.0.1:5432/bank",
)
REPLICA_ID = os.getenv("REPLICA_ID", "unknown")
SHARD_ID = os.getenv("SHARD_ID", "unknown")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", "50"))

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
    action: str = Field(..., description="balance | deposit | withdraw")
    account: str = Field(..., description="Account ID, e.g. A0500")
    amount: Optional[Any] = Field(default=None)


app = FastAPI(title="Distributed Bank Shard Replica")


@app.on_event("startup")
def on_startup() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    logger.info(
        f"{utc_now()}  server_start  replica={REPLICA_ID}  shard={SHARD_ID}  "
        f"listening={HOST}:{PORT}"
    )


@app.get("/health")
def health() -> dict:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {
            "status": "ok",
            "replica_id": REPLICA_ID,
            "shard_id": SHARD_ID,
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


@app.post("/transaction")
def transaction(body: TransactionRequest, request: Request):
    started = time.perf_counter()
    request_id = uuid.uuid4().hex[:8]
    client_ip = _client_ip(request)
    action = (body.action or "").strip().lower()
    account = (body.account or "").strip()

    if not _client_slots.acquire(blocking=False):
        return _error("error", "server busy: max concurrent clients reached", 503, account or None)

    try:
        if not account:
            return _error("error", "missing account field", 400)
        if action not in {"balance", "deposit", "withdraw"}:
            return _error("error", f"unsupported action: {body.action}", 400, account)

        try:
            with db_transaction() as cur:
                if action == "balance":
                    cur.execute(
                        "SELECT account_id, balance FROM accounts WHERE account_id = %s",
                        (account,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise LookupError("unknown_account")
                    result_balance = Decimal(row["balance"])
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
                    result_balance = balance

            latency = (time.perf_counter() - started) * 1000
            logger.info(
                f"{utc_now()}  req={request_id}  ip={client_ip}  "
                f"replica={REPLICA_ID}  shard={SHARD_ID}  "
                f"action={action}  account={account}  result=success  "
                f"balance={result_balance}  latency={latency:.0f}ms"
            )
            return {
                "status": "success",
                "account": account,
                "balance": float(result_balance),
                "request_id": request_id,
                "replica_id": REPLICA_ID,
                "shard_id": SHARD_ID,
            }

        except LookupError:
            return _error("error", f"unknown account: {account}", 404, account)
        except ValueError as exc:
            messages = {
                "missing_amount": "missing amount field",
                "invalid_amount": "amount must be a positive number",
            }
            return _error("error", messages.get(str(exc), "invalid request"), 400, account)
        except RuntimeError as exc:
            if str(exc) == "insufficient_funds":
                return _error("error", "insufficient funds", 400, account)
            raise
        except Exception:
            logger.exception("transaction failed")
            return _error(
                "error", "internal server error; no balance change applied", 500, account
            )
    finally:
        _client_slots.release()


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
