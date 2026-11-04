"""Architecture lab console: live DB snapshot + gateway proxy + Docker status."""

from __future__ import annotations

import http.client
import json
import os
import socket
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

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
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "2")
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")

STATIC = Path(__file__).parent / "static"
app = FastAPI(title="Bank Architecture Lab")
app.mount("/assets", StaticFiles(directory=STATIC), name="assets")


class TxBody(BaseModel):
    action: str
    account: str
    amount: Optional[Any] = None
    to_account: Optional[str] = Field(default=None)


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, sock_path: str, timeout: float = 5.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self.sock_path = sock_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.sock_path)
        self.sock = sock


def docker_api(path: str, timeout: float = 5.0) -> Any:
    if not Path(DOCKER_SOCK).exists():
        raise FileNotFoundError(f"Docker socket not found: {DOCKER_SOCK}")
    conn = UnixHTTPConnection(DOCKER_SOCK, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status >= 400:
            raise RuntimeError(f"Docker API {resp.status}: {body.decode('utf-8', 'replace')}")
        if not body:
            return None
        return json.loads(body.decode("utf-8"))
    finally:
        conn.close()


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


def _fmt_ports(ports: Optional[list[dict]]) -> list[str]:
    if not ports:
        return []
    out: list[str] = []
    for p in ports:
        public = p.get("PublicPort")
        private = p.get("PrivatePort")
        proto = p.get("Type", "tcp")
        if public:
            ip = p.get("IP") or "0.0.0.0"
            out.append(f"{ip}:{public}->{private}/{proto}")
        elif private:
            out.append(f"{private}/{proto}")
    # de-dupe while preserving order
    seen = set()
    unique = []
    for item in out:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def _state_kind(state: str, health: Optional[str]) -> str:
    s = (state or "").lower()
    h = (health or "").lower()
    if s == "running" and h == "healthy":
        return "healthy"
    if s == "running" and h == "unhealthy":
        return "unhealthy"
    if s == "running" and h == "starting":
        return "starting"
    if s == "running":
        return "running"
    if s in {"exited", "dead", "created", "paused", "restarting", "removing"}:
        return s
    return s or "unknown"


def collect_docker_status() -> dict[str, Any]:
    project = COMPOSE_PROJECT
    filters = quote(json.dumps({"label": [f"com.docker.compose.project={project}"]}))

    containers_raw = docker_api(f"/containers/json?all=1&filters={filters}") or []
    containers = []
    for c in containers_raw:
        labels = c.get("Labels") or {}
        names = [n.lstrip("/") for n in (c.get("Names") or [])]
        health = None
        status_text = c.get("Status") or ""
        # Health is in State when inspecting; list endpoint puts it in Status string sometimes.
        state_obj = c.get("State")
        if isinstance(state_obj, dict):
            health = (state_obj.get("Health") or {}).get("Status")
            state = state_obj.get("Status") or c.get("State")
        else:
            state = state_obj or "unknown"
            if "(healthy)" in status_text:
                health = "healthy"
            elif "(unhealthy)" in status_text:
                health = "unhealthy"
            elif "(health: starting)" in status_text:
                health = "starting"

        containers.append(
            {
                "id": (c.get("Id") or "")[:12],
                "name": names[0] if names else labels.get("com.docker.compose.service", "?"),
                "service": labels.get("com.docker.compose.service"),
                "image": c.get("Image"),
                "state": state,
                "health": health,
                "status": status_text,
                "kind": _state_kind(str(state), health),
                "ports": _fmt_ports(c.get("Ports")),
                "created": c.get("Created"),
            }
        )

    containers.sort(key=lambda x: (x.get("service") or x["name"]))

    volumes_raw = docker_api(f"/volumes?filters={filters}") or {}
    volumes = []
    for v in volumes_raw.get("Volumes") or []:
        labels = v.get("Labels") or {}
        volumes.append(
            {
                "name": v.get("Name"),
                "driver": v.get("Driver"),
                "mountpoint": v.get("Mountpoint"),
                "service": labels.get("com.docker.compose.volume"),
            }
        )
    volumes.sort(key=lambda x: x.get("name") or "")

    networks_raw = docker_api(f"/networks?filters={filters}") or []
    # Also include the default project network by name pattern if label filter misses it.
    if not networks_raw:
        all_nets = docker_api("/networks") or []
        networks_raw = [
            n
            for n in all_nets
            if (n.get("Labels") or {}).get("com.docker.compose.project") == project
            or (n.get("Name") or "").startswith(f"{project}_")
        ]

    networks = []
    for n in networks_raw:
        networks.append(
            {
                "id": (n.get("Id") or "")[:12],
                "name": n.get("Name"),
                "driver": n.get("Driver"),
                "scope": n.get("Scope"),
            }
        )
    networks.sort(key=lambda x: x.get("name") or "")

    counts = {
        "total": len(containers),
        "running": sum(1 for c in containers if c["state"] == "running"),
        "healthy": sum(1 for c in containers if c["kind"] == "healthy"),
        "unhealthy": sum(1 for c in containers if c["kind"] == "unhealthy"),
        "exited": sum(1 for c in containers if c["state"] == "exited"),
        "other": sum(
            1
            for c in containers
            if c["state"] not in {"running", "exited"}
        ),
    }

    return {
        "ok": True,
        "project": project,
        "counts": counts,
        "containers": containers,
        "volumes": volumes,
        "networks": networks,
    }


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


@app.get("/api/docker")
def docker_status():
    try:
        return collect_docker_status()
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "project": COMPOSE_PROJECT,
                "error": str(exc),
                "hint": "Mount /var/run/docker.sock into the console container.",
                "counts": {},
                "containers": [],
                "volumes": [],
                "networks": [],
            },
        )


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
