"""
Witness Node (Arbiter)
Lightweight service that prevents split brain.

Two capabilities:
  1. `/ping`, `/health` — simple reachability for majority counting.
  2. `/lease/*`        — exclusive leader lease. Only one server can hold the
                         lease at a time, so even when both servers can still
                         reach the witness (A-B link cut), only one of them is
                         allowed to write.

Run on a separate VM (e.g., 192.168.56.103:9000).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel, Field
import uvicorn

from envfile import load_env_file

# Load config
load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

HOST = os.getenv("WITNESS_HOST", "0.0.0.0")
PORT = int(os.getenv("WITNESS_PORT", "9000"))
LEASE_MAX_TTL = float(os.getenv("LEASE_MAX_TTL", "30"))

# Simple logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WITNESS] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("witness")

app = FastAPI(title="Witness Node")


# ---------------------------------------------------------------------------
# Lease state (one leader at a time)
# ---------------------------------------------------------------------------

_lease_lock = threading.Lock()
_lease = {"leader": None, "expires_at": 0.0}


class LeaseRequest(BaseModel):
    node: str = Field(..., min_length=1, description="Unique server id")
    ttl: float = Field(default=5.0, gt=0, description="Lease duration in seconds")


def _remaining(now: float) -> float:
    return max(0.0, round(_lease["expires_at"] - now, 3))


def _clamp_ttl(ttl: float) -> float:
    return max(1.0, min(float(ttl), LEASE_MAX_TTL))


# ---------------------------------------------------------------------------
# Reachability endpoints (used by majority counting)
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok", "role": "witness"}


@app.get("/ping")
def ping() -> dict:
    """Simple ping endpoint for quorum checks."""
    return {
        "status": "alive",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Lease endpoints (used for exclusive leadership)
# ---------------------------------------------------------------------------

@app.post("/lease/acquire")
def lease_acquire(req: LeaseRequest) -> dict:
    """
    Grant the lease if nobody holds a valid lease.
    A node may re-acquire its own live lease (idempotent).
    """
    ttl = _clamp_ttl(req.ttl)
    now = time.monotonic()

    with _lease_lock:
        current = _lease["leader"]
        expired = now >= _lease["expires_at"]

        if current is None or current == req.node or expired:
            previous = current
            _lease["leader"] = req.node
            _lease["expires_at"] = now + ttl
            if previous != req.node:
                logger.info(
                    f"lease acquired by {req.node} (previous={previous}) ttl={ttl}s"
                )
            return {
                "granted": True,
                "leader": req.node,
                "ttl": ttl,
                "expires_in": _remaining(now),
            }

        logger.info(f"lease denied to {req.node}: held by {current}")
        return {
            "granted": False,
            "leader": current,
            "expires_in": _remaining(now),
        }


@app.post("/lease/renew")
def lease_renew(req: LeaseRequest) -> dict:
    """Extend the lease. Only the current leader may renew."""
    ttl = _clamp_ttl(req.ttl)
    now = time.monotonic()

    with _lease_lock:
        if _lease["leader"] != req.node:
            return {
                "granted": False,
                "leader": _lease["leader"],
                "expires_in": _remaining(now),
            }

        _lease["expires_at"] = now + ttl
        return {
            "granted": True,
            "leader": req.node,
            "ttl": ttl,
            "expires_in": _remaining(now),
        }


@app.post("/lease/release")
def lease_release(req: LeaseRequest) -> dict:
    """Voluntarily give up the lease (used on clean shutdown)."""
    with _lease_lock:
        if _lease["leader"] != req.node:
            return {"released": False, "leader": _lease["leader"]}

        _lease["leader"] = None
        _lease["expires_at"] = 0.0
        logger.info(f"lease released by {req.node}")
        return {"released": True, "leader": None}


@app.get("/lease/status")
def lease_status() -> dict:
    """Current leader and remaining lease time."""
    now = time.monotonic()
    with _lease_lock:
        leader = _lease["leader"]
        expires_in = _remaining(now)
        if leader is not None and expires_in <= 0:
            leader = None
        return {
            "leader": leader,
            "expires_in": expires_in,
            "active": leader is not None,
        }


@app.on_event("startup")
def on_startup() -> None:
    logger.info(f"Witness node started on {HOST}:{PORT}")


if __name__ == "__main__":
    uvicorn.run("witness:app", host=HOST, port=PORT, reload=False)
