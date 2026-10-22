"""
Mini Banking Transaction Server
- HTTP (FastAPI)
- PostgreSQL for durable shared state + ACID
- Concurrent clients with row-level locks (SELECT ... FOR UPDATE)
- Configurable MAX_CLIENTS via environment variable
- Quorum-based primary election to prevent split brain
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException
import requests

from envfile import load_env_file

# Load settings from .env next to this file (no need for export)
load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://bankuser:bankpass@127.0.0.1:5432/minibank",
)
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", "20"))
LOG_FILE = os.getenv("LOG_FILE", "bank_server.log")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# Quorum configuration for split-brain prevention
WITNESS_URL = os.getenv("WITNESS_URL", "")  # e.g., http://192.168.56.12:9000
PEER_SERVER_URL = os.getenv("PEER_SERVER_URL", "")  # e.g., http://192.168.56.11:8000
QUORUM_TIMEOUT = float(os.getenv("QUORUM_TIMEOUT", "2.0"))  # seconds
ENABLE_QUORUM = os.getenv("ENABLE_QUORUM", "false").lower() == "true"

# Lease-based leader election (stronger than quorum: exclusive leader)
# When enabled, a server may only write while it holds the witness lease.
ENABLE_LEASE = os.getenv("ENABLE_LEASE", "false").lower() == "true"
NODE_ID = os.getenv("NODE_ID") or f"{socket.gethostname()}:{PORT}"
LEASE_TTL = float(os.getenv("LEASE_TTL", "6.0"))  # seconds
LEASE_RENEW_INTERVAL = float(os.getenv("LEASE_RENEW_INTERVAL", "2.0"))  # seconds

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
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_primary_db() -> None:
    """If DB is in standby/replica mode, promote it to primary so writes succeed."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                row = cur.fetchone()
                if row and row.get("pg_is_in_recovery"):
                    logger.info("Replica detected: promoting database to primary...")
                    cur.execute("SELECT pg_promote()")
                    conn.commit()
                    time.sleep(0.5)
    except Exception as exc:
        logger.warning(f"Could not promote DB (may already be primary or permission issue): {exc}")


@contextmanager
def db_transaction():
    """One DB transaction: BEGIN on enter, COMMIT on success, ROLLBACK on error."""
    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                yield cur


# ---------------------------------------------------------------------------
# Quorum check for split-brain prevention
# ---------------------------------------------------------------------------

def has_quorum() -> bool:
    """
    Check if this server has quorum (majority of nodes reachable).
    Quorum = 2 out of 3 nodes (self + witness + peer).
    If quorum is disabled, always return True.
    """
    if not ENABLE_QUORUM:
        return True
    
    reachable = 1  # self is always reachable
    
    # Check witness
    if WITNESS_URL:
        try:
            resp = requests.get(
                f"{WITNESS_URL.rstrip('/')}/ping",
                timeout=QUORUM_TIMEOUT,
            )
            if resp.status_code == 200:
                reachable += 1
        except requests.RequestException:
            pass
    
    # Check peer server
    if PEER_SERVER_URL:
        try:
            resp = requests.get(
                f"{PEER_SERVER_URL.rstrip('/')}/health",
                timeout=QUORUM_TIMEOUT,
            )
            if resp.status_code == 200:
                reachable += 1
        except requests.RequestException:
            pass
    
    # Need 2 out of 3 for quorum
    return reachable >= 2


# ---------------------------------------------------------------------------
# Lease-based leader election (exclusive primary)
# ---------------------------------------------------------------------------
# Quorum counting alone has a hole: if the A-B link is cut but both servers
# can still reach the witness, both would count 2 votes and both would think
# they are primary. The witness lease fixes this by granting leadership to
# exactly one node at a time.

_lease_lock = threading.Lock()
_lease_state: dict[str, Any] = {
    "is_leader": False,
    "expires_at": 0.0,
    "leader": None,
}
_lease_stop = threading.Event()


def _witness_post(path: str, payload: dict) -> Optional[dict]:
    """POST to the witness lease API; return JSON or None on failure."""
    if not WITNESS_URL:
        return None
    try:
        resp = requests.post(
            f"{WITNESS_URL.rstrip('/')}{path}",
            json=payload,
            timeout=QUORUM_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        return resp.json()
    except (requests.RequestException, ValueError):
        return None


def _lease_tick() -> None:
    """One lease cycle: renew if leader, otherwise try to acquire."""
    if not ENABLE_LEASE:
        return

    now = time.monotonic()
    with _lease_lock:
        was_leader = _lease_state["is_leader"]
        current_expiry = _lease_state["expires_at"]

    if was_leader:
        data = _witness_post("/lease/renew", {"node": NODE_ID, "ttl": LEASE_TTL})
        with _lease_lock:
            if data and data.get("granted"):
                _lease_state["expires_at"] = now + LEASE_TTL
                _lease_state["leader"] = data.get("leader", NODE_ID)
                return

            # Renewal refused → we are no longer the leader.
            if data is not None:
                _lease_state["is_leader"] = False
                _lease_state["leader"] = data.get("leader")
                logger.warning(
                    f"lease lost: leader is now {data.get('leader')}; entering read-only"
                )
                return

            # Witness unreachable → keep serving until the local lease expires.
            if now >= current_expiry:
                _lease_state["is_leader"] = False
                logger.warning("lease expired while witness unreachable; read-only")
        return

    data = _witness_post("/lease/acquire", {"node": NODE_ID, "ttl": LEASE_TTL})
    if data and data.get("granted"):
        with _lease_lock:
            _lease_state["is_leader"] = True
            _lease_state["expires_at"] = now + LEASE_TTL
            _lease_state["leader"] = data.get("leader", NODE_ID)
        logger.info(f"lease acquired by {NODE_ID}")


def _lease_loop() -> None:
    while not _lease_stop.is_set():
        try:
            _lease_tick()
        except Exception as exc:  # never let the loop die
            logger.warning(f"lease loop error: {exc}")
        _lease_stop.wait(LEASE_RENEW_INTERVAL)


def has_lease() -> bool:
    """True when this server currently holds a valid write lease."""
    if not ENABLE_LEASE:
        return True
    with _lease_lock:
        return _lease_state["is_leader"] and time.monotonic() < _lease_state["expires_at"]


def lease_snapshot() -> dict[str, Any]:
    with _lease_lock:
        now = time.monotonic()
        return {
            "enabled": ENABLE_LEASE,
            "node_id": NODE_ID,
            "is_leader": _lease_state["is_leader"],
            "leader": _lease_state["leader"],
            "expires_in": max(0.0, round(_lease_state["expires_at"] - now, 3)),
        }


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

    # Take leadership before announcing readiness (fail-closed if witness is down)
    if ENABLE_LEASE:
        if not WITNESS_URL:
            logger.warning(
                "ENABLE_LEASE=true but WITNESS_URL is empty: all writes will be "
                "rejected (read-only) because leadership cannot be acquired"
            )
        _lease_tick()
        threading.Thread(target=_lease_loop, name="lease-loop", daemon=True).start()

    logger.info(
        f"{utc_now()}  server_start  max_clients={MAX_CLIENTS}  "
        f"db=connected  listening={HOST}:{PORT}  node={NODE_ID}  "
        f"lease={'enabled' if ENABLE_LEASE else 'disabled'}"
    )


@app.on_event("shutdown")
def on_shutdown() -> None:
    _lease_stop.set()
    if ENABLE_LEASE:
        _witness_post("/lease/release", {"node": NODE_ID})


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/quorum-status")
def quorum_status() -> dict:
    """Check current quorum and lease status."""
    quorum_ok = has_quorum()
    lease_ok = has_lease()
    can_write = quorum_ok and lease_ok

    if not ENABLE_QUORUM and not ENABLE_LEASE:
        mode = "standalone"
    else:
        mode = "primary" if can_write else "read-only"

    return {
        "quorum_enabled": ENABLE_QUORUM,
        "has_quorum": quorum_ok,
        "lease": lease_snapshot(),
        "can_write": can_write,
        "mode": mode,
        "witness_url": WITNESS_URL or None,
        "peer_url": PEER_SERVER_URL or None,
    }


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

        # Quorum + lease check for write operations (deposit/withdraw)
        if action in {"deposit", "withdraw"}:
            if not has_quorum():
                latency = (time.perf_counter() - started) * 1000
                write_log(
                    request_id=request_id,
                    client_ip=client_ip,
                    action=action,
                    account=account,
                    result="error:no_quorum",
                    latency_ms=latency,
                    extra="read-only mode: network partition detected",
                )
                return _error(
                    "error",
                    "no quorum: server in read-only mode due to network partition",
                    503,
                    account,
                )
            # Exclusive leader check: prevents two primaries writing when both
            # can still reach the witness (the quorum-counting blind spot).
            if not has_lease():
                latency = (time.perf_counter() - started) * 1000
                write_log(
                    request_id=request_id,
                    client_ip=client_ip,
                    action=action,
                    account=account,
                    result="error:not_leader",
                    latency_ms=latency,
                    extra="read-only mode: witness lease not held",
                )
                return _error(
                    "error",
                    "not primary: server in read-only mode (lease not held)",
                    503,
                    account,
                )
            # Ensure DB is promoted if this replica took over as primary
            ensure_primary_db()

        try:
            with db_transaction() as cur:
                if action == "balance":
                    # Read-only query: no lock needed, works cleanly on both primary and standby
                    cur.execute(
                        "SELECT account_id, balance FROM accounts WHERE account_id = %s",
                        (account,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise LookupError("unknown_account")
                    result_balance = Decimal(row["balance"])
                else:
                    # Write operation: lock the row for isolation
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
    uvicorn.run("server:app", host=HOST, port=PORT, reload=False)
