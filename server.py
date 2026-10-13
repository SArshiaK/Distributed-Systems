"""
Mini Banking Transaction Server
- HTTP (FastAPI)
- PostgreSQL for durable shared state + ACID
- Concurrent clients with row-level locks (SELECT ... FOR UPDATE)
- Configurable MAX_CLIENTS via environment variable
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://bankuser:bankpass@127.0.0.1:5432/minibank",
)
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", "20"))
LOG_FILE = os.getenv("LOG_FILE", "bank_server.log")

# Limits how many transaction handlers run at the same time
_client_slots = threading.Semaphore(MAX_CLIENTS)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("minibank")
logger.setLevel(logging.INFO)
logger.handlers.clear()

_fmt = logging.Formatter("%(message)s")
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_sh)
logger.addHandler(_fh)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def write_log(
    *,
    request_id: str,
    client_ip: str,
    action: Any,
    account: Any,
    result: str,
    latency_ms: float,
    extra: str = "",
) -> None:
    line = (
        f"{utc_now()}  req={request_id}  ip={client_ip}  "
        f"action={action}  account={account}  result={result}  "
        f"latency={latency_ms:.0f}ms"
    )
    if extra:
        line += f"  {extra}"
    logger.info(line)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(DATABASE_URL)


@contextmanager
def db_transaction():
    """One DB transaction: BEGIN on enter, COMMIT on success, ROLLBACK on error."""
    conn = get_connection()
    try:
        # psycopg2 starts a transaction automatically; we commit/rollback explicitly
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class TransactionRequest(BaseModel):
    action: str = Field(..., description="balance | deposit | withdraw")
    account: str = Field(..., description="Account ID, e.g. A1001")
    amount: Optional[Any] = Field(
        default=None,
        description="Required for deposit/withdraw; ignored for balance",
    )


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Mini Banking Transaction Server")


@app.on_event("startup")
def on_startup() -> None:
    # Fail fast if DB is unreachable
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    logger.info(
        f"{utc_now()}  server_start  max_clients={MAX_CLIENTS}  "
        f"db=connected  listening=0.0.0.0:8000"
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _client_ip(request: Request) -> str:
    if request.client:
        return request.client.host
    return "unknown"


def _parse_amount(raw: Any) -> Decimal:
    """Validate amount: must be a positive number."""
    if raw is None:
        raise ValueError("missing_amount")
    try:
        amount = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("invalid_amount") from None
    if amount <= 0:
        raise ValueError("invalid_amount")
    return amount.quantize(Decimal("0.01"))


def _error(status: str, message: str, http_code: int, account: Optional[str] = None) -> JSONResponse:
    body: dict[str, Any] = {"status": status, "message": message}
    if account is not None:
        body["account"] = account
    return JSONResponse(status_code=http_code, content=body)


@app.post("/transaction")
def transaction(body: TransactionRequest, request: Request):
    """
    Main banking endpoint.
    Uses a semaphore for MAX_CLIENTS and PostgreSQL row locks for ACID.
    """
    started = time.perf_counter()
    request_id = uuid.uuid4().hex[:8]
    client_ip = _client_ip(request)
    action = (body.action or "").strip().lower()
    account = (body.account or "").strip()

    acquired = _client_slots.acquire(blocking=False)
    if not acquired:
        latency = (time.perf_counter() - started) * 1000
        write_log(
            request_id=request_id,
            client_ip=client_ip,
            action=action,
            account=account,
            result="error:server_busy",
            latency_ms=latency,
        )
        return _error("error", "server busy: max concurrent clients reached", 503, account or None)

    try:
        if not account:
            latency = (time.perf_counter() - started) * 1000
            write_log(
                request_id=request_id,
                client_ip=client_ip,
                action=action,
                account=account,
                result="error:missing_account",
                latency_ms=latency,
            )
            return _error("error", "missing account field", 400)

        if action not in {"balance", "deposit", "withdraw"}:
            latency = (time.perf_counter() - started) * 1000
            write_log(
                request_id=request_id,
                client_ip=client_ip,
                action=action,
                account=account,
                result="error:unsupported_action",
                latency_ms=latency,
            )
            return _error("error", f"unsupported action: {body.action}", 400, account)

        try:
            with db_transaction() as cur:
                # Lock the row so concurrent ops on same account are serialized (Isolation)
                cur.execute(
                    "SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE",
                    (account,),
                )
                row = cur.fetchone()
                if row is None:
                    # ROLLBACK happens automatically when we leave without commit... 
                    # With `with conn:` context, raising or returning after no writes is fine.
                    # Explicit raise to abort cleanly:
                    raise LookupError("unknown_account")

                balance = Decimal(row["balance"])

                if action == "balance":
                    result_balance = balance
                else:
                    amount = _parse_amount(body.amount)

                    if action == "deposit":
                        balance = balance + amount
                        cur.execute(
                            "UPDATE accounts SET balance = %s WHERE account_id = %s",
                            (balance, account),
                        )
                        result_balance = balance
                    else:  # withdraw
                        if balance < amount:
                            raise RuntimeError("insufficient_funds")
                        balance = balance - amount
                        cur.execute(
                            "UPDATE accounts SET balance = %s WHERE account_id = %s",
                            (balance, account),
                        )
                        result_balance = balance

                # Leaving the `with conn:` block commits → Durability before reply

            latency = (time.perf_counter() - started) * 1000
            write_log(
                request_id=request_id,
                client_ip=client_ip,
                action=action,
                account=account,
                result="success",
                latency_ms=latency,
                extra=f"balance={result_balance}",
            )
            return {
                "status": "success",
                "account": account,
                "balance": float(result_balance),
                "request_id": request_id,
            }

        except LookupError:
            latency = (time.perf_counter() - started) * 1000
            write_log(
                request_id=request_id,
                client_ip=client_ip,
                action=action,
                account=account,
                result="error:unknown_account",
                latency_ms=latency,
            )
            return _error("error", f"unknown account: {account}", 404, account)

        except ValueError as exc:
            code = str(exc)
            messages = {
                "missing_amount": "missing amount field",
                "invalid_amount": "amount must be a positive number",
            }
            msg = messages.get(code, "invalid request")
            latency = (time.perf_counter() - started) * 1000
            write_log(
                request_id=request_id,
                client_ip=client_ip,
                action=action,
                account=account,
                result=f"error:{code}",
                latency_ms=latency,
            )
            return _error("error", msg, 400, account)

        except RuntimeError as exc:
            if str(exc) == "insufficient_funds":
                latency = (time.perf_counter() - started) * 1000
                write_log(
                    request_id=request_id,
                    client_ip=client_ip,
                    action=action,
                    account=account,
                    result="error:insufficient_funds",
                    latency_ms=latency,
                )
                return _error("error", "insufficient funds", 400, account)
            raise

        except Exception as exc:
            # Any unexpected failure → transaction rolled back (Atomicity)
            latency = (time.perf_counter() - started) * 1000
            write_log(
                request_id=request_id,
                client_ip=client_ip,
                action=action,
                account=account,
                result=f"error:internal:{type(exc).__name__}",
                latency_ms=latency,
            )
            return _error("error", "internal server error; no balance change applied", 500, account)

    finally:
        _client_slots.release()


# ---------------------------------------------------------------------------
# Global error handlers (malformed JSON, missing fields, etc.)
# ---------------------------------------------------------------------------

@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    started_header = request.headers.get("x-request-started")
    _ = started_header  # unused; latency measured roughly here
    request_id = uuid.uuid4().hex[:8]
    client_ip = _client_ip(request)
    write_log(
        request_id=request_id,
        client_ip=client_ip,
        action="?",
        account="?",
        result="error:validation",
        latency_ms=0,
        extra=str(exc.errors())[:200],
    )
    return JSONResponse(
        status_code=400,
        content={
            "status": "error",
            "message": "invalid or missing fields",
            "details": exc.errors(),
            "request_id": request_id,
        },
    )


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException):
    request_id = uuid.uuid4().hex[:8]
    client_ip = _client_ip(request)
    write_log(
        request_id=request_id,
        client_ip=client_ip,
        action="?",
        account="?",
        result=f"error:http_{exc.status_code}",
        latency_ms=0,
        extra=str(exc.detail)[:200],
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": str(exc.detail), "request_id": request_id},
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception):
    # Covers broken JSON body / unexpected failures while keeping server alive
    request_id = uuid.uuid4().hex[:8]
    client_ip = _client_ip(request)
    write_log(
        request_id=request_id,
        client_ip=client_ip,
        action="?",
        account="?",
        result=f"error:malformed_or_unhandled:{type(exc).__name__}",
        latency_ms=0,
        extra=str(exc)[:200],
    )
    return JSONResponse(
        status_code=400,
        content={
            "status": "error",
            "message": "malformed request",
            "request_id": request_id,
        },
    )


if __name__ == "__main__":
    import uvicorn

    # IMPORTANT: workers=1 (default). Do NOT use --workers > 1 for this assignment demo.
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
