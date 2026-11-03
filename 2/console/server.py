"""Architecture lab console: live DB snapshot + gateway proxy."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import psycopg
import requests
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "3000"))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8080")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8090")
DB_A_URL = os.getenv("DB_A_URL", "postgresql://bank:bank@db-a:5432/bank")
DB_B_URL = os.getenv("DB_B_URL", "postgresql://bank:bank@db-b:5432/bank")
DB_COORD_URL = os.getenv("DB_COORD_URL", "postgresql://bank:bank@db-coord:5432/coord")
WATCH_ACCOUNTS = os.getenv(
    "WATCH_ACCOUNTS",
    "A0100,A0200,A0500,A0600,A1000,A1001,A1500,A1600,A2000",
).split(",")

STATIC = Path(__file__).parent / "static"
app = FastAPI(title="Bank Architecture Lab")
app.mount("/assets", StaticFiles(directory=STATIC), name="assets")


class TxBody(BaseModel):
    action: str
    account: str
    amount: Optional[Any] = None
    to_account: Optional[str] = Field(default=None)


def fetch_accounts(url: str, shard: str) -> list[dict]:
    ids = [a.strip() for a in WATCH_ACCOUNTS if a.strip()]
    with psycopg.connect(url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT account_id, balance FROM accounts "
                "WHERE account_id = ANY(%s) ORDER BY account_id",
                (ids,),
            )
            rows = cur.fetchall()
    return [
        {
            "account_id": r["account_id"],
            "balance": float(r["balance"]),
            "shard": shard,
        }
        for r in rows
    ]


def fetch_coord_recent(limit: int = 8) -> list[dict]:
    try:
        with psycopg.connect(DB_COORD_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT transaction_id, from_account, to_account, amount, "
                    "from_shard, to_shard, status, updated_at "
                    "FROM distributed_transactions "
                    "ORDER BY updated_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["amount"] = float(item["amount"])
            item["updated_at"] = item["updated_at"].isoformat()
            out.append(item)
        return out
    except Exception:
        return []


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "console"}


@app.get("/api/snapshot")
def snapshot():
    gateway = {}
    try:
        gateway = requests.get(f"{GATEWAY_URL.rstrip('/')}/shard-map", timeout=3).json()
    except requests.RequestException as exc:
        gateway = {"error": str(exc)}

    coord_health = {"status": "unknown"}
    try:
        coord_health = requests.get(
            f"{COORDINATOR_URL.rstrip('/')}/health",
            timeout=2,
        ).json()
    except requests.RequestException as exc:
        coord_health = {"status": "error", "message": str(exc)}

    try:
        accounts_a = fetch_accounts(DB_A_URL, "A")
        accounts_b = fetch_accounts(DB_B_URL, "B")
        db_ok = True
        db_error = None
    except Exception as exc:
        accounts_a, accounts_b = [], []
        db_ok = False
        db_error = str(exc)

    return {
        "gateway": gateway,
        "coordinator": coord_health,
        "databases": {
            "ok": db_ok,
            "error": db_error,
            "A": accounts_a,
            "B": accounts_b,
        },
        "recent_2pc": fetch_coord_recent(),
    }


@app.post("/api/transaction")
def transaction(body: TxBody):
    try:
        r = requests.post(
            f"{GATEWAY_URL.rstrip('/')}/transaction",
            json=body.model_dump(),
            timeout=15,
        )
        try:
            data = r.json()
        except ValueError:
            data = {"status": "error", "message": r.text}
    except requests.RequestException as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": f"gateway unreachable: {exc}"},
        )

    # Enrich with animation route hints for the UI.
    action = body.action.strip().lower()
    route = ["client", "gateway"]
    if action == "transfer" and body.to_account:
        mode = data.get("mode")
        if mode == "cross_shard" or data.get("decision"):
            route = ["client", "gateway", "coordinator", "replica_from", "db_from", "replica_to", "db_to"]
        else:
            route = ["client", "gateway", "replica", "db"]
    else:
        route = ["client", "gateway", "replica", "db"]

    if isinstance(data, dict):
        data["_route"] = route
        data["_action"] = action
    return JSONResponse(status_code=r.status_code, content=data)


@app.get("/api/transactions/{tx_id}")
def tx_status(tx_id: str):
    try:
        r = requests.get(
            f"{GATEWAY_URL.rstrip('/')}/transactions/{tx_id}",
            timeout=5,
        )
        return JSONResponse(status_code=r.status_code, content=r.json())
    except requests.RequestException as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": str(exc)},
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
