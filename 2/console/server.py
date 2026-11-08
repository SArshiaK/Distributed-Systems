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

import bonus_live
import lab_experiments

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "3000"))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8080")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8090")
MIGRATOR_URL = os.getenv("MIGRATOR_URL", "http://migrator:8095")
HEALER_URL = os.getenv("HEALER_URL", "http://healer:8096")
DB_A_URL = os.getenv("DB_A_URL", "postgresql://bank:bank@db-a:5432/bank")
DB_B_URL = os.getenv("DB_B_URL", "postgresql://bank:bank@db-b:5432/bank")
DB_C_URL = os.getenv("DB_C_URL", "postgresql://bank:bank@db-c:5432/bank")
DB_COORD_URL = os.getenv("DB_COORD_URL", "postgresql://bank:bank@db-coord:5432/coord")
BANK_B_ADMIN = os.getenv("BANK_B_ADMIN", "http://bank-b1:8000")
BANK_C_ADMIN = os.getenv("BANK_C_ADMIN", "http://bank-c1:8000")
WATCH_ACCOUNTS = os.getenv(
    "WATCH_ACCOUNTS",
    "A0100,A0110,A0120,A0200,A0300,A0500,A0600,A0820,A1000,"
    "A1001,A1500,A1510,A1520,A1600,A1820,A2000",
).split(",")
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "2")
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
PROJECT_NETWORK = os.getenv("COMPOSE_NETWORK", f"{COMPOSE_PROJECT}_default")
FAULT_DIR = os.getenv("FAULT_DIR", "/faults")

# Lab controls: replicas + gateway/coordinator for failover / 2PC experiments.
CONTROLLABLE_SERVICES = {
    "bank-a1",
    "bank-a2",
    "bank-b1",
    "bank-b2",
    "bank-c1",
    "bank-c2",
    "gateway",
    "coordinator",
}
PROTECTED_SERVICES = {"console"}

STATIC = Path(__file__).parent / "static"
app = FastAPI(title="Bank Architecture Lab")


@app.middleware("http")
async def disable_static_cache(request, call_next):
    """Console UI breaks hard when browsers keep a stale app.js/index.html."""
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.startswith("/assets/"):
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    return response


app.mount("/assets", StaticFiles(directory=STATIC), name="assets")


class TxBody(BaseModel):
    action: str
    account: str
    amount: Optional[Any] = None
    to_account: Optional[str] = Field(default=None)


class ContainerActionBody(BaseModel):
    action: str = Field(..., description="start | stop | isolate | rejoin")


class LabRunBody(BaseModel):
    experiment: int = Field(..., description="3 | 6 | 7")


class BonusMigrateBody(BaseModel):
    force_new: bool = True


class HealEnableBody(BaseModel):
    enabled: bool = True


class HealCrashBody(BaseModel):
    slot_id: Optional[str] = None
    service: Optional[str] = None
    mode: str = "stop"


bonus_live.configure(
    gateway_url=GATEWAY_URL,
    coordinator_url=COORDINATOR_URL,
    migrator_url=MIGRATOR_URL,
    db_a_url=DB_A_URL,
    db_b_url=DB_B_URL,
    db_c_url=DB_C_URL,
    db_coord_url=DB_COORD_URL,
    bank_b_admin=BANK_B_ADMIN,
    bank_c_admin=BANK_C_ADMIN,
)


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, sock_path: str, timeout: float = 5.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self.sock_path = sock_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.sock_path)
        self.sock = sock


def docker_api(
    path: str,
    method: str = "GET",
    payload: Optional[dict] = None,
    timeout: float = 15.0,
) -> Any:
    if not Path(DOCKER_SOCK).exists():
        raise FileNotFoundError(f"Docker socket not found: {DOCKER_SOCK}")
    conn = UnixHTTPConnection(DOCKER_SOCK, timeout=timeout)
    try:
        body = None
        headers = {}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        if resp.status >= 400:
            raise RuntimeError(
                f"Docker API {resp.status}: {raw.decode('utf-8', 'replace')}"
            )
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))
    finally:
        conn.close()


lab_experiments.configure(
    gateway_url=GATEWAY_URL,
    coordinator_url=COORDINATOR_URL,
    db_a_url=DB_A_URL,
    db_b_url=DB_B_URL,
    db_coord_url=DB_COORD_URL,
    fault_dir=FAULT_DIR,
    compose_project=COMPOSE_PROJECT,
    docker_api=docker_api,
)


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


def fetch_coord_snapshot(limit: int = 12) -> dict[str, Any]:
    """Live Coord DB view: recent distributed txs + their participants."""
    try:
        with psycopg.connect(DB_COORD_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT transaction_id, from_account, to_account, amount, "
                    "from_shard, to_shard, status, created_at, updated_at "
                    "FROM distributed_transactions "
                    "ORDER BY updated_at DESC LIMIT %s",
                    (limit,),
                )
                txs = list(cur.fetchall())
                tx_ids = [t["transaction_id"] for t in txs]
                participants: list[dict] = []
                if tx_ids:
                    cur.execute(
                        "SELECT transaction_id, shard_id, operation, account, vote, ack "
                        "FROM transaction_participants "
                        "WHERE transaction_id = ANY(%s) "
                        "ORDER BY transaction_id, shard_id",
                        (tx_ids,),
                    )
                    participants = list(cur.fetchall())
                cur.execute("SELECT COUNT(*) AS n FROM distributed_transactions")
                total = int(cur.fetchone()["n"])
                cur.execute(
                    "SELECT status, COUNT(*) AS n FROM distributed_transactions "
                    "GROUP BY status ORDER BY status"
                )
                by_status = {r["status"]: int(r["n"]) for r in cur.fetchall()}

        out_txs = []
        for r in txs:
            item = dict(r)
            item["amount"] = float(item["amount"])
            item["created_at"] = item["created_at"].isoformat()
            item["updated_at"] = item["updated_at"].isoformat()
            out_txs.append(item)

        return {
            "ok": True,
            "total": total,
            "by_status": by_status,
            "transactions": out_txs,
            "participants": participants,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "total": 0,
            "by_status": {},
            "transactions": [],
            "participants": [],
        }

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


def _inspect_container(container_ref: str) -> dict:
    return docker_api(f"/containers/{container_ref}/json")


def _assert_project_container(info: dict) -> str:
    labels = info.get("Config", {}).get("Labels") or info.get("Labels") or {}
    project = labels.get("com.docker.compose.project")
    if project != COMPOSE_PROJECT:
        raise PermissionError("container is not part of this compose project")
    service = labels.get("com.docker.compose.service") or ""
    if service in PROTECTED_SERVICES:
        raise PermissionError(f"service '{service}' is protected from lab controls")
    if service not in CONTROLLABLE_SERVICES:
        raise PermissionError(f"service '{service}' is not controllable from the UI")
    return service


def _network_names(info: dict) -> set[str]:
    networks = (info.get("NetworkSettings") or {}).get("Networks") or {}
    return set(networks.keys())


def _is_isolated(info: dict) -> bool:
    nets = _network_names(info)
    return PROJECT_NETWORK not in nets


def apply_container_action(container_ref: str, action: str) -> dict[str, Any]:
    action = (action or "").strip().lower()
    if action not in {"start", "stop", "isolate", "rejoin"}:
        raise ValueError("action must be start, stop, isolate, or rejoin")

    info = _inspect_container(container_ref)
    service = _assert_project_container(info)
    full_id = info["Id"]
    name = (info.get("Name") or "").lstrip("/")
    state = ((info.get("State") or {}).get("Status") or "").lower()

    if action == "stop":
        if state != "running":
            return {
                "ok": True,
                "action": action,
                "service": service,
                "name": name,
                "message": f"{name} already stopped",
            }
        docker_api(f"/containers/{full_id}/stop?t=3", method="POST")
        return {
            "ok": True,
            "action": action,
            "service": service,
            "name": name,
            "message": f"stopped {name}",
        }

    if action == "start":
        if state == "running":
            # If previously isolated, keep current network state.
            return {
                "ok": True,
                "action": action,
                "service": service,
                "name": name,
                "message": f"{name} already running",
                "isolated": _is_isolated(info),
            }
        docker_api(f"/containers/{full_id}/start", method="POST")
        return {
            "ok": True,
            "action": action,
            "service": service,
            "name": name,
            "message": f"started {name}",
        }

    if action == "isolate":
        if state != "running":
            raise RuntimeError("container must be running to isolate it")
        if _is_isolated(info):
            return {
                "ok": True,
                "action": action,
                "service": service,
                "name": name,
                "message": f"{name} already isolated from {PROJECT_NETWORK}",
                "isolated": True,
            }
        docker_api(
            f"/networks/{PROJECT_NETWORK}/disconnect",
            method="POST",
            payload={"Container": full_id, "Force": True},
        )
        return {
            "ok": True,
            "action": action,
            "service": service,
            "name": name,
            "message": f"disconnected {name} from {PROJECT_NETWORK}",
            "isolated": True,
        }

    # rejoin — restore Compose DNS aliases so peers can resolve service names again.
    if state != "running":
        raise RuntimeError("container must be running to rejoin the network")
    if not _is_isolated(info):
        return {
            "ok": True,
            "action": action,
            "service": service,
            "name": name,
            "message": f"{name} already on {PROJECT_NETWORK}",
            "isolated": False,
        }
    aliases = []
    for alias in (service, name, f"{COMPOSE_PROJECT}-{service}-1"):
        if alias and alias not in aliases:
            aliases.append(alias)
    docker_api(
        f"/networks/{PROJECT_NETWORK}/connect",
        method="POST",
        payload={
            "Container": full_id,
            "EndpointConfig": {"Aliases": aliases},
        },
    )
    return {
        "ok": True,
        "action": action,
        "service": service,
        "name": name,
        "message": f"reconnected {name} to {PROJECT_NETWORK}",
        "isolated": False,
        "aliases": aliases,
    }


def collect_docker_status() -> dict[str, Any]:
    project = COMPOSE_PROJECT
    filters = quote(json.dumps({"label": [f"com.docker.compose.project={project}"]}))

    containers_raw = docker_api(f"/containers/json?all=1&filters={filters}") or []
    containers = []
    for c in containers_raw:
        labels = c.get("Labels") or {}
        names = [n.lstrip("/") for n in (c.get("Names") or [])]
        service = labels.get("com.docker.compose.service")
        health = None
        status_text = c.get("Status") or ""
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

        networks = list(
            (((c.get("NetworkSettings") or {}).get("Networks")) or {}).keys()
        )
        # List endpoint may omit Networks; fall back to inspect for controllable ones.
        isolated = False
        if str(state) == "running" and service in CONTROLLABLE_SERVICES:
            if PROJECT_NETWORK in networks:
                isolated = False
            elif networks:
                isolated = True
            else:
                try:
                    isolated = _is_isolated(_inspect_container(c["Id"]))
                except Exception:
                    isolated = False

        kind = _state_kind(str(state), health)
        if isolated and str(state) == "running":
            kind = "isolated"

        managed = str(labels.get("healer.managed", "")).lower() in {"1", "true", "yes"}
        slot = labels.get("healer.slot") or ""
        generation = labels.get("healer.generation") or ""
        # Healer replacements may wrongly inherit compose.service=bank-*; show slot instead.
        if managed:
            service = slot or labels.get("healer.shard") or service or "heal"
        controllable = (service in CONTROLLABLE_SERVICES) or (
            managed and str(labels.get("healer.shard", "")) in {"A", "B", "C"}
        )
        containers.append(
            {
                "id": (c.get("Id") or "")[:12],
                "name": names[0] if names else service or "?",
                "service": service,
                "image": c.get("Image"),
                "state": state,
                "health": health,
                "status": status_text,
                "kind": kind,
                "isolated": isolated,
                "controllable": controllable,
                "managed": managed,
                "slot": slot or None,
                "generation": generation or None,
                "ports": _fmt_ports(c.get("Ports")),
                "created": c.get("Created"),
            }
        )

    # Mark superseded healer containers (older generation still running for same slot).
    newest_gen: dict[str, int] = {}
    for item in containers:
        if not item.get("managed") or not item.get("slot"):
            continue
        try:
            gen = int(item.get("generation") or 0)
        except ValueError:
            gen = 0
        slot_id = str(item["slot"])
        if gen >= newest_gen.get(slot_id, -1):
            newest_gen[slot_id] = gen
    for item in containers:
        if not item.get("managed") or not item.get("slot"):
            item["orphan"] = False
            continue
        try:
            gen = int(item.get("generation") or 0)
        except ValueError:
            gen = 0
        item["orphan"] = gen < newest_gen.get(str(item["slot"]), gen)

    # Compose services first, then active heals, orphans last.
    def _sort_key(x: dict[str, Any]) -> tuple:
        managed = 1 if x.get("managed") else 0
        orphan = 1 if x.get("orphan") else 0
        return (managed, orphan, x.get("service") or x["name"])

    containers.sort(key=_sort_key)

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
        "running": sum(1 for c in containers if c["state"] == "running" and not c["isolated"]),
        "healthy": sum(1 for c in containers if c["kind"] == "healthy"),
        "unhealthy": sum(1 for c in containers if c["kind"] == "unhealthy"),
        "isolated": sum(1 for c in containers if c.get("isolated")),
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
        "network": PROJECT_NETWORK,
        "counts": counts,
        "containers": containers,
        "volumes": volumes,
        "networks": networks,
    }


@app.get("/")
def index():
    return FileResponse(
        STATIC / "index.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
        },
    )


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
        "coord_db": (coord_snap := fetch_coord_snapshot()),
        # Keep alias for older UI builds.
        "recent_2pc": coord_snap.get("transactions", []),
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


@app.post("/api/docker/containers/{container_id}/action")
def docker_container_action(container_id: str, body: ContainerActionBody):
    try:
        result = apply_container_action(container_id, body.action)
        return result
    except PermissionError as exc:
        return JSONResponse(
            status_code=403,
            content={"ok": False, "error": str(exc)},
        )
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": str(exc)},
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(exc)},
        )


def _enrich_flow_hints(body: TxBody, data: dict, *, gateway_unreachable: bool = False) -> dict:
    """Attach animation hints: where the request actually got, and where it failed."""
    action = (body.action or "").strip().lower()
    msg = str(data.get("message") or "").lower()
    status = str(data.get("status") or "").lower()
    decision = str(data.get("decision") or "").upper()

    ok = status == "success" or decision == "COMMIT"
    pending = status in {"pending", "unknown"} or decision == "" and status == "pending"

    fail_at = None
    if gateway_unreachable or "gateway unreachable" in msg:
        fail_at = "gateway"
    elif "coordinator unreachable" in msg:
        fail_at = "coordinator"
    elif "no healthy replica" in msg:
        fail_at = "gateway"
    elif "upstream replica unreachable" in msg:
        fail_at = "replica"
    elif not ok and not pending and status in {"error", "vote_no"} and not (
        data.get("replica_id") or data.get("gateway_replica") or data.get("mode")
    ):
        # Generic transport/routing failure before a concrete participant answered.
        if "coordinator" in msg:
            fail_at = "coordinator"
        elif "replica" in msg or "shard" in msg:
            fail_at = "replica"

    reached = ["client"]
    if fail_at != "gateway":
        reached.append("gateway")
    else:
        reached.append("gateway")
        data["_route"] = reached
        data["_fail_at"] = "gateway"
        data["_action"] = action
        data["_ok"] = False
        return data

    is_cross = bool(
        action == "transfer"
        and body.to_account
        and (
            data.get("mode") == "cross_shard"
            or decision in {"COMMIT", "ABORT"}
            or fail_at == "coordinator"
            or "cross" in msg
        )
    )

    # Infer cross-shard intent even when coordinator never answered.
    if (
        not is_cross
        and action == "transfer"
        and body.to_account
        and fail_at == "coordinator"
    ):
        is_cross = True

    if is_cross or fail_at == "coordinator":
        reached.append("coordinator")
        if fail_at == "coordinator":
            data["_route"] = reached
            data["_fail_at"] = "coordinator"
            data["_action"] = action
            data["_ok"] = False
            return data
        # Detailed 2PC path including durable Coord DB writes.
        if ok or decision == "ABORT" or pending or data.get("mode") == "cross_shard":
            reached.extend(
                [
                    "db_coord_create",  # INITIATED + participants
                    "replica_from",
                    "db_from",
                    "db_coord_vote_from",  # vote persist
                    "replica_to",
                    "db_to",
                    "db_coord_vote_to",  # vote persist
                    "db_coord_decision",  # COMMIT_DECIDED / ABORT_DECIDED
                ]
            )
            if ok or decision == "ABORT":
                reached.extend(
                    [
                        "replica_from_decide",
                        "replica_to_decide",
                        "db_coord_complete",  # COMPLETED + acks
                    ]
                )
            elif pending or status == "pending":
                # Decision durable; waiting on participant acks.
                reached.append("db_coord_pending")
    else:
        if fail_at == "replica":
            reached.append("replica")
            data["_route"] = reached
            data["_fail_at"] = "replica"
            data["_action"] = action
            data["_ok"] = False
            return data
        # Local path completed to a replica (success or business error).
        if (
            ok
            or data.get("replica_id")
            or data.get("gateway_replica")
            or data.get("mode") == "same_shard"
            or status in {"error", "aborted"}
        ):
            reached.extend(["replica", "db"])

    data["_route"] = reached
    data["_fail_at"] = fail_at
    data["_action"] = action
    data["_ok"] = ok
    return data


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
        if not isinstance(data, dict):
            data = {"status": "error", "message": str(data)}
        data = _enrich_flow_hints(body, data)
        return JSONResponse(status_code=r.status_code, content=data)
    except requests.RequestException as exc:
        data = _enrich_flow_hints(
            body,
            {"status": "error", "message": f"gateway unreachable: {exc}"},
            gateway_unreachable=True,
        )
        return JSONResponse(status_code=503, content=data)


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


@app.get("/api/lab/status")
def lab_status():
    return lab_experiments.get_status()


@app.post("/api/lab/run")
def lab_run(body: LabRunBody):
    result = lab_experiments.start(body.experiment)
    if not result.get("ok"):
        return JSONResponse(status_code=409, content=result)
    return result


@app.get("/api/bonus/live")
def bonus_live_snapshot():
    return bonus_live.snapshot()


@app.post("/api/bonus/traffic/start")
def bonus_traffic_start():
    return bonus_live.start_traffic()


@app.post("/api/bonus/traffic/stop")
def bonus_traffic_stop():
    return bonus_live.stop_traffic()


@app.post("/api/bonus/migrate/start")
def bonus_migrate_start(body: BonusMigrateBody = BonusMigrateBody()):
    result = bonus_live.start_migration(force_new=body.force_new)
    if result.get("ok") is False:
        return JSONResponse(status_code=409, content=result)
    return result


@app.post("/api/bonus/migrate/stop")
def bonus_migrate_stop():
    result = bonus_live.stop_migration()
    if result.get("ok") is False:
        return JSONResponse(status_code=500, content=result)
    return result


@app.post("/api/bonus/migrate/resume")
def bonus_migrate_resume():
    result = bonus_live.resume_migration()
    if result.get("ok") is False:
        code = 404 if "no open" in str(result.get("error", "")).lower() else 409
        return JSONResponse(status_code=code, content=result)
    return result


@app.post("/api/bonus/logs/clear")
def bonus_logs_clear():
    return bonus_live.clear_logs()


@app.post("/api/bonus/reset")
def bonus_reset():
    result = bonus_live.reset_demo_state()
    if not result.get("ok"):
        return JSONResponse(status_code=500, content=result)
    return result


def _healer_proxy(method: str, path: str, payload: Optional[dict] = None):
    try:
        r = requests.request(
            method,
            f"{HEALER_URL.rstrip('/')}{path}",
            json=payload,
            timeout=20,
        )
        try:
            data = r.json()
        except ValueError:
            data = {"ok": False, "error": r.text}
        if r.status_code >= 400:
            return JSONResponse(status_code=r.status_code, content=data)
        return data
    except requests.RequestException as exc:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": f"healer unreachable: {exc}"},
        )


@app.get("/api/heal/status")
def heal_status():
    return _healer_proxy("GET", "/status")


@app.post("/api/heal/enable")
def heal_enable(body: HealEnableBody):
    return _healer_proxy("POST", "/enable", {"enabled": body.enabled})


@app.post("/api/heal/traffic/start")
def heal_traffic_start():
    return _healer_proxy("POST", "/traffic/start")


@app.post("/api/heal/traffic/stop")
def heal_traffic_stop():
    return _healer_proxy("POST", "/traffic/stop")


@app.post("/api/heal/inject/crash")
def heal_inject_crash(body: HealCrashBody):
    return _healer_proxy(
        "POST",
        "/inject/crash",
        {"slot_id": body.slot_id, "service": body.service, "mode": body.mode},
    )


@app.post("/api/heal/reconcile")
def heal_reconcile():
    return _healer_proxy("POST", "/reconcile")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
