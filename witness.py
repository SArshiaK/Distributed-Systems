"""
Witness Node (Arbiter)
Lightweight service for quorum voting to prevent split brain.
Run on a separate VM (e.g., 192.168.56.12:9000).
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

from fastapi import FastAPI
import uvicorn

from envfile import load_env_file

# Load config
load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

HOST = os.getenv("WITNESS_HOST", "0.0.0.0")
PORT = int(os.getenv("WITNESS_PORT", "9000"))

# Simple logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WITNESS] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("witness")

app = FastAPI(title="Witness Node")


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


@app.on_event("startup")
def on_startup() -> None:
    logger.info(f"Witness node started on {HOST}:{PORT}")


if __name__ == "__main__":
    uvicorn.run("witness:app", host=HOST, port=PORT, reload=False)
