"""
Bonus II: Autonomous Self-Healing Controller.

Detects failed/unresponsive bank replicas, spawns replacement containers itself
(no Swarm / K8s / restart policies), and registers them in the shard map so the
Gateway and Coordinator keep serving via remaining healthy replicas.

Crash-safe: replacement intent is persisted before create; on restart the same
deterministic container name is adopted instead of spawning a duplicate.
"""

from __future__ import annotations

import http.client
import json
import logging
import os
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8096"))
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "2")
PROJECT_NETWORK = os.getenv("COMPOSE_NETWORK", f"{COMPOSE_PROJECT}_default")
SHARD_MAP_PATH = Path(os.getenv("SHARD_MAP_PATH", "/config/shard_map.json"))
STATE_PATH = Path(os.getenv("HEALER_STATE_PATH", "/config/healer_state.json"))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8080").rstrip("/")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8090").rstrip("/")
POLL_SECS = float(os.getenv("HEALER_POLL_SECS", "2"))
FAIL_THRESHOLD = int(os.getenv("HEALER_FAIL_THRESHOLD", "2"))
REPLACE_DELAY_SECS = float(os.getenv("HEALER_REPLACE_DELAY_SECS", "2.5"))
DESIRED_A = int(os.getenv("HEALER_DESIRED_A", "2"))
DESIRED_B = int(os.getenv("HEALER_DESIRED_B", "2"))
ENABLED_DEFAULT = os.getenv("HEALER_ENABLED", "true").lower() in {"1", "true", "yes"}

# Seed templates: compose service → shard + slot
SEED_SLOTS = [
    {"shard": "A", "slot_id": "A-0", "service": "bank-a1", "dns": "bank-a1"},
    {"shard": "A", "slot_id": "A-1", "service": "bank-a2", "dns": "bank-a2"},
    {"shard": "B", "slot_id": "B-0", "service": "bank-b1", "dns": "bank-b1"},
    {"shard": "B", "slot_id": "B-1", "service": "bank-b2", "dns": "bank-b2"},
]

logger = logging.getLogger("healer")
logger.setLevel(logging.INFO)
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)

_lock = threading.Lock()
_stop = threading.Event()
_events: list[dict[str, Any]] = []
_fail_streak: dict[str, int] = {}
_state: dict[str, Any] = {
    "enabled": ENABLED_DEFAULT,
    "desired": {"A": DESIRED_A, "B": DESIRED_B},
    "slots": {},
    "pending": None,
    "replacing": False,
}


def log_event(level: str, message: str, **extra: Any) -> None:
    entry = {"t": time.time(), "level": level, "message": message, **extra}
    with _lock:
        _events.append(entry)
        if len(_events) > 400:
            del _events[: len(_events) - 400]
    logger.info(f"{level.upper():5}  {message}")


# ---- Docker HTTP over unix socket -------------------------------------------------


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, sock_path: str, timeout: float = 30.0) -> None:
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
    timeout: float = 45.0,
) -> Any:
    if not Path(DOCKER_SOCK).exists():
        raise FileNotFoundError(f"Docker socket not found: {DOCKER_SOCK}")
    conn = UnixHTTPConnection(DOCKER_SOCK, timeout=timeout)
    try:
        body = None
        headers: dict[str, str] = {}
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


def docker_list_project_containers() -> list[dict]:
    filters = quote(json.dumps({"label": [f"com.docker.compose.project={COMPOSE_PROJECT}"]}))
    return docker_api(f"/containers/json?all=1&filters={filters}") or []


def docker_find_by_name(name: str) -> Optional[dict]:
    for c in docker_list_project_containers():
        names = [n.lstrip("/") for n in (c.get("Names") or [])]
        if name in names:
            return c
        labels = c.get("Labels") or {}
        if labels.get("healer.container_name") == name:
            return c
    # Also accept absolute inspect by name
    try:
        return docker_api(f"/containers/{name}/json")
    except Exception:
        return None


def docker_inspect(ref: str) -> dict:
    return docker_api(f"/containers/{ref}/json")


def docker_service_container(service: str) -> Optional[dict]:
    for c in docker_list_project_containers():
        labels = c.get("Labels") or {}
        if labels.get("com.docker.compose.service") == service:
            return c
    return None


# ---- State persistence ------------------------------------------------------------


def _default_slots() -> dict[str, list[dict[str, Any]]]:
    slots: dict[str, list[dict[str, Any]]] = {"A": [], "B": []}
    for seed in SEED_SLOTS:
        slots[seed["shard"]].append(
            {
                "slot_id": seed["slot_id"],
                "shard": seed["shard"],
                "service": seed["service"],
                "dns": seed["dns"],
                "url": f"http://{seed['dns']}:8000",
                "container_name": None,
                "generation": 1,
                "managed": False,
                "status": "unknown",
            }
        )
    return slots


def save_state() -> None:
    with _lock:
        payload = {
            "enabled": _state["enabled"],
            "desired": dict(_state["desired"]),
            "slots": _state["slots"],
            "pending": _state["pending"],
        }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    text = json.dumps(payload, indent=2) + "\n"
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass
    tmp.replace(STATE_PATH)


def load_state() -> None:
    global _state
    if STATE_PATH.exists():
        try:
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            with _lock:
                _state["enabled"] = bool(raw.get("enabled", ENABLED_DEFAULT))
                _state["desired"] = raw.get("desired") or {"A": DESIRED_A, "B": DESIRED_B}
                _state["slots"] = raw.get("slots") or _default_slots()
                _state["pending"] = raw.get("pending")
            log_event("info", "loaded durable healer state")
            return
        except Exception as exc:  # noqa: BLE001
            log_event("fail", f"state load failed, reseeding: {exc}")
    with _lock:
        _state["slots"] = _default_slots()
        _state["pending"] = None
    save_state()


def bind_seed_containers() -> None:
    """Attach compose container names to seed slots once."""
    changed = False
    with _lock:
        slots = _state["slots"]
    for shard, rows in slots.items():
        for slot in rows:
            if slot.get("container_name"):
                continue
            svc = slot.get("service")
            if not svc:
                continue
            c = docker_service_container(svc)
            if not c:
                continue
            names = [n.lstrip("/") for n in (c.get("Names") or [])]
            slot["container_name"] = names[0] if names else None
            changed = True
    if changed:
        with _lock:
            _state["slots"] = slots
        save_state()


# ---- Shard map registration -------------------------------------------------------


def load_map() -> dict:
    return json.loads(SHARD_MAP_PATH.read_text(encoding="utf-8"))


def write_map(data: dict) -> None:
    text = json.dumps(data, indent=2) + "\n"
    SHARD_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = SHARD_MAP_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass
    # Windows bind mounts sometimes reject rename over open files — try replace, else write in place.
    try:
        tmp.replace(SHARD_MAP_PATH)
    except OSError:
        with SHARD_MAP_PATH.open("w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def register_replica(shard: str, old_url: str, new_url: str) -> dict:
    """Swap failed replica URL for replacement; leave other replicas untouched."""
    raw = load_map()
    shards = raw.setdefault("shards", {})
    if shard not in shards:
        raise RuntimeError(f"shard {shard} missing from map")
    reps = list(shards[shard].get("replicas") or [])
    if new_url in reps:
        # Already registered (idempotent after crash).
        if old_url in reps and old_url != new_url:
            reps = [u for u in reps if u != old_url]
            shards[shard]["replicas"] = reps
            write_map(raw)
    else:
        if old_url in reps:
            reps = [new_url if u == old_url else u for u in reps]
        else:
            reps.append(new_url)
        # Deduplicate while preserving order
        seen = set()
        deduped = []
        for u in reps:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        shards[shard]["replicas"] = deduped
        write_map(raw)

    reload_routers()
    return {"shard": shard, "replicas": shards[shard]["replicas"]}


def reload_routers() -> None:
    for base in (GATEWAY_URL, COORDINATOR_URL):
        try:
            requests.post(f"{base}/admin/reload-map", timeout=5)
        except requests.RequestException as exc:
            log_event("info", f"reload {base} deferred: {exc}")


# ---- Health / probe ---------------------------------------------------------------


def probe_http(url: str, timeout: float = 1.5) -> bool:
    try:
        r = requests.get(f"{url.rstrip('/')}/health", timeout=timeout)
        return r.status_code == 200 and r.json().get("status") == "ok"
    except (requests.RequestException, ValueError, TypeError):
        return False


def container_running(name: Optional[str]) -> bool:
    if not name:
        return False
    try:
        info = docker_inspect(name)
        return ((info.get("State") or {}).get("Status") or "").lower() == "running"
    except Exception:
        return False


def slot_healthy(slot: dict) -> bool:
    url_ok = probe_http(slot["url"])
    # Prefer HTTP truth — shared DB replicas are healthy if app answers.
    return url_ok


# ---- Replacement ------------------------------------------------------------------


def _template_for_shard(shard: str) -> dict:
    """Inspect a live compose replica to clone Image + Env."""
    prefer = "bank-a1" if shard == "A" else "bank-b1"
    c = docker_service_container(prefer)
    if not c:
        # Fall back to any running slot of that shard
        with _lock:
            for slot in _state["slots"].get(shard, []):
                if slot.get("container_name") and container_running(slot["container_name"]):
                    c = docker_find_by_name(slot["container_name"])
                    if c:
                        break
    if not c:
        raise RuntimeError(f"no template container for shard {shard}")
    info = docker_inspect(c["Id"])
    cfg = info.get("Config") or {}
    image = cfg.get("Image") or info.get("Image")
    env = list(cfg.get("Env") or [])
    return {"image": image, "env": env, "id": info["Id"]}


def _env_with_replica(env: list[str], replica_id: str, shard: str) -> list[str]:
    out = []
    seen = set()
    overrides = {
        "REPLICA_ID": replica_id,
        "SHARD_ID": shard,
        "HOST": "0.0.0.0",
        "PORT": "8000",
    }
    for item in env:
        key = item.split("=", 1)[0]
        if key in overrides:
            out.append(f"{key}={overrides[key]}")
            seen.add(key)
        else:
            out.append(item)
            seen.add(key)
    for k, v in overrides.items():
        if k not in seen:
            out.append(f"{k}={v}")
    return out


def create_replacement(slot: dict, generation: int) -> dict:
    shard = slot["shard"]
    slot_id = slot["slot_id"]
    dns = f"heal-{slot_id.lower()}-g{generation}"
    name = f"{COMPOSE_PROJECT}-{dns}"
    url = f"http://{dns}:8000"
    replica_id = f"{shard}-H{generation}"

    # Crash recovery: if we already created this exact name, adopt it.
    existing = docker_find_by_name(name)
    if existing:
        log_event("info", f"adopting existing replacement {name} (no duplicate)", slot=slot_id)
        state = (existing.get("State") or "")
        if isinstance(state, dict):
            running = state.get("Status") == "running"
        else:
            running = str(state).lower() == "running"
        if not running:
            docker_api(f"/containers/{existing['Id']}/start", method="POST")
        return {
            "container_name": name,
            "dns": dns,
            "url": url,
            "generation": generation,
            "adopted": True,
        }

    tmpl = _template_for_shard(shard)
    env = _env_with_replica(tmpl["env"], replica_id, shard)
    labels = {
        "com.docker.compose.project": COMPOSE_PROJECT,
        "healer.managed": "true",
        "healer.slot": slot_id,
        "healer.shard": shard,
        "healer.generation": str(generation),
        "healer.container_name": name,
    }
    body = {
        "Image": tmpl["image"],
        "Env": env,
        "Labels": labels,
        "HostConfig": {
            "RestartPolicy": {"Name": "no"},  # required: no Docker auto-restart
        },
        "NetworkingConfig": {
            "EndpointsConfig": {
                PROJECT_NETWORK: {
                    "Aliases": [dns, name],
                }
            }
        },
    }
    log_event("phase", f"creating replacement {name} for {slot_id}", slot=slot_id)
    created = docker_api(f"/containers/create?name={quote(name)}", method="POST", payload=body)
    cid = created["Id"]
    docker_api(f"/containers/{cid}/start", method="POST")
    return {
        "container_name": name,
        "dns": dns,
        "url": url,
        "generation": generation,
        "adopted": False,
        "id": cid,
    }


def wait_healthy_url(url: str, timeout: float = 45.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if probe_http(url):
            return True
        time.sleep(0.5)
    return False


def stop_remove_container(name: Optional[str]) -> None:
    if not name:
        return
    try:
        info = docker_inspect(name)
        cid = info["Id"]
        state = ((info.get("State") or {}).get("Status") or "").lower()
        if state == "running":
            docker_api(f"/containers/{cid}/stop?t=2", method="POST")
        docker_api(f"/containers/{cid}?force=true", method="DELETE")
        log_event("info", f"removed failed container {name}")
    except Exception as exc:  # noqa: BLE001
        log_event("info", f"cleanup {name} skipped: {exc}")


def perform_replace(slot: dict) -> None:
    """Persist pending intent → create/adopt → register → cleanup old."""
    with _lock:
        if _state["replacing"]:
            return
        _state["replacing"] = True
        for rows in _state["slots"].values():
            for s in rows:
                if s["slot_id"] == slot["slot_id"]:
                    s["status"] = "unhealthy"

    try:
        # Brief pause so the UI can show the down replica before cutover.
        if REPLACE_DELAY_SECS > 0:
            log_event(
                "phase",
                f"holding {REPLACE_DELAY_SECS:.1f}s so peers keep traffic while {slot['slot_id']} stays down",
                slot=slot["slot_id"],
            )
            time.sleep(REPLACE_DELAY_SECS)

        generation = int(slot.get("generation") or 1) + 1
        old_url = slot["url"]
        old_name = slot.get("container_name")
        pending = {
            "type": "replace",
            "slot_id": slot["slot_id"],
            "shard": slot["shard"],
            "old_url": old_url,
            "old_container": old_name,
            "generation": generation,
            "new_dns": f"heal-{slot['slot_id'].lower()}-g{generation}",
            "new_name": f"{COMPOSE_PROJECT}-heal-{slot['slot_id'].lower()}-g{generation}",
            "new_url": f"http://heal-{slot['slot_id'].lower()}-g{generation}:8000",
            "started_at": time.time(),
        }
        with _lock:
            _state["pending"] = pending
        save_state()  # durable before create — prevents duplicates after crash
        log_event(
            "phase",
            f"replace intent persisted for {slot['slot_id']} gen={generation}",
            slot=slot["slot_id"],
        )

        created = create_replacement(slot, generation)
        if not wait_healthy_url(created["url"], timeout=50):
            raise RuntimeError(f"replacement {created['url']} never became healthy")

        register_replica(slot["shard"], old_url, created["url"])
        log_event(
            "pass",
            f"registered {created['url']} for shard {slot['shard']} (replaced {old_url})",
            slot=slot["slot_id"],
        )

        # Update slot bookkeeping
        with _lock:
            for rows in _state["slots"].values():
                for s in rows:
                    if s["slot_id"] == slot["slot_id"]:
                        s["url"] = created["url"]
                        s["dns"] = created["dns"]
                        s["container_name"] = created["container_name"]
                        s["generation"] = generation
                        s["managed"] = True
                        s["service"] = None
                        s["status"] = "healthy"
                        break
            _state["pending"] = None
        save_state()

        # Remove the failed container only after map points at the new one.
        if old_name and old_name != created["container_name"]:
            stop_remove_container(old_name)
        _fail_streak[slot["slot_id"]] = 0
        log_event("pass", f"slot {slot['slot_id']} healed → {created['dns']}", slot=slot["slot_id"])
    except Exception as exc:  # noqa: BLE001
        log_event("fail", f"replace failed for {slot.get('slot_id')}: {exc}")
        # Keep pending so crash recovery / next loop can retry same name
    finally:
        with _lock:
            _state["replacing"] = False


def resume_pending() -> None:
    with _lock:
        pending = _state.get("pending")
        slots = _state["slots"]
        if _state["replacing"]:
            return
    if not pending or pending.get("type") != "replace":
        return
    slot_id = pending["slot_id"]
    slot = None
    for rows in slots.values():
        for s in rows:
            if s["slot_id"] == slot_id:
                slot = s
                break
    if not slot:
        with _lock:
            _state["pending"] = None
        save_state()
        return

    log_event("info", f"resuming pending replace for {slot_id} (controller recovery)")
    new_url = pending["new_url"]
    new_name = pending["new_name"]
    new_dns = pending["new_dns"]
    generation = int(pending["generation"])
    old_url = pending["old_url"]
    old_name = pending.get("old_container")

    # Case 1: replacement already healthy — finish registration without a new create.
    if probe_http(new_url) or container_running(new_name):
        if not probe_http(new_url):
            # Container up but app not ready yet — wait briefly.
            wait_healthy_url(new_url, timeout=40)
        if probe_http(new_url):
            register_replica(pending["shard"], old_url, new_url)
            with _lock:
                for s in _state["slots"].get(pending["shard"], []):
                    if s["slot_id"] == slot_id:
                        s["url"] = new_url
                        s["dns"] = new_dns
                        s["container_name"] = new_name
                        s["generation"] = generation
                        s["managed"] = True
                        s["service"] = None
                        s["status"] = "healthy"
                        break
                _state["pending"] = None
            save_state()
            if old_name and old_name != new_name:
                stop_remove_container(old_name)
            _fail_streak[slot_id] = 0
            log_event("pass", f"recovered pending replace for {slot_id} → {new_dns}")
            return

    # Case 2: same deterministic name — create/adopt then register (no gen bump).
    with _lock:
        _state["replacing"] = True
    try:
        slot_view = dict(slot)
        slot_view["generation"] = generation - 1
        created = create_replacement(slot_view, generation)
        if not wait_healthy_url(created["url"], timeout=50):
            raise RuntimeError(f"replacement {created['url']} never became healthy")
        register_replica(pending["shard"], old_url, created["url"])
        with _lock:
            for s in _state["slots"].get(pending["shard"], []):
                if s["slot_id"] == slot_id:
                    s["url"] = created["url"]
                    s["dns"] = created["dns"]
                    s["container_name"] = created["container_name"]
                    s["generation"] = generation
                    s["managed"] = True
                    s["service"] = None
                    s["status"] = "healthy"
                    break
            _state["pending"] = None
        save_state()
        if old_name and old_name != created["container_name"]:
            stop_remove_container(old_name)
        _fail_streak[slot_id] = 0
        log_event("pass", f"resumed replace completed for {slot_id} → {created['dns']}")
    except Exception as exc:  # noqa: BLE001
        log_event("fail", f"resume replace failed for {slot_id}: {exc}")
    finally:
        with _lock:
            _state["replacing"] = False


# ---- Control loop -----------------------------------------------------------------


def reconcile_once() -> None:
    bind_seed_containers()
    with _lock:
        enabled = _state["enabled"]
        slots = {k: [dict(s) for s in v] for k, v in _state["slots"].items()}
        pending = _state.get("pending")
        replacing = _state["replacing"]

    if pending and not replacing:
        resume_pending()
        return

    if not enabled or replacing:
        # Still refresh status for UI
        _refresh_statuses(slots)
        return

    for shard, rows in slots.items():
        for slot in rows:
            sid = slot["slot_id"]
            ok = slot_healthy(slot)
            with _lock:
                for s in _state["slots"].get(shard, []):
                    if s["slot_id"] == sid:
                        s["status"] = "healthy" if ok else "unhealthy"
                        s["container_running"] = container_running(s.get("container_name"))
            if ok:
                _fail_streak[sid] = 0
                continue
            _fail_streak[sid] = _fail_streak.get(sid, 0) + 1
            streak = _fail_streak[sid]
            log_event(
                "info",
                f"slot {sid} unhealthy streak={streak}/{FAIL_THRESHOLD} url={slot['url']}",
                slot=sid,
            )
            if streak >= FAIL_THRESHOLD:
                # Snapshot fresh slot from state
                with _lock:
                    live = next(
                        (s for s in _state["slots"].get(shard, []) if s["slot_id"] == sid),
                        slot,
                    )
                perform_replace(dict(live))
                return  # one replacement per tick — keep impact bounded


def _refresh_statuses(slots: dict) -> None:
    for shard, rows in slots.items():
        for slot in rows:
            ok = slot_healthy(slot)
            with _lock:
                for s in _state["slots"].get(shard, []):
                    if s["slot_id"] == slot["slot_id"]:
                        s["status"] = "healthy" if ok else "unhealthy"
                        s["container_running"] = container_running(s.get("container_name"))


def control_loop() -> None:
    log_event("info", "self-healing control loop started")
    while not _stop.is_set():
        try:
            reconcile_once()
        except Exception as exc:  # noqa: BLE001
            log_event("fail", f"reconcile error: {exc}")
        _stop.wait(POLL_SECS)


# ---- Demo traffic (for UI) --------------------------------------------------------

_traffic_on = False
_traffic_thread: Optional[threading.Thread] = None
_traffic_stats = {"sent": 0, "ok": 0, "fail": 0}
_traffic_log: list[dict[str, Any]] = []
_traffic_lock = threading.Lock()


def _traffic_worker() -> None:
    accounts = ["A0100", "A0500", "A0800", "A1200", "A1500", "A1800"]
    i = 0
    while True:
        with _traffic_lock:
            if not _traffic_on:
                return
        acc = accounts[i % len(accounts)]
        i += 1
        action = "deposit" if i % 2 == 0 else "withdraw"
        rid = uuid.uuid4().hex
        row: dict[str, Any] = {
            "t": time.time(),
            "action": action,
            "account": acc,
            "ok": False,
            "status": None,
            "replica": None,
            "id": rid[:8],
        }
        try:
            r = requests.post(
                f"{GATEWAY_URL}/transaction",
                json={
                    "action": action,
                    "account": acc,
                    "amount": 1,
                    "client_request_id": rid,
                },
                timeout=8,
            )
            data = r.json() if r.content else {}
            st = str(data.get("status") or "").lower()
            row["ok"] = r.status_code == 200 and st in {
                "ok",
                "success",
                "committed",
                "completed",
            }
            row["status"] = r.status_code
            row["replica"] = data.get("gateway_replica") or data.get("replica_id")
        except requests.RequestException as exc:
            row["error"] = str(exc)
        with _traffic_lock:
            _traffic_stats["sent"] += 1
            if row["ok"]:
                _traffic_stats["ok"] += 1
            else:
                _traffic_stats["fail"] += 1
            _traffic_log.append(row)
            if len(_traffic_log) > 200:
                del _traffic_log[: len(_traffic_log) - 200]
        time.sleep(0.35)


def start_traffic() -> dict:
    global _traffic_on, _traffic_thread
    with _traffic_lock:
        _traffic_on = True
        if _traffic_thread and _traffic_thread.is_alive():
            return {"ok": True, "running": True}
        _traffic_thread = threading.Thread(target=_traffic_worker, daemon=True, name="heal-traffic")
        _traffic_thread.start()
    return {"ok": True, "running": True}


def stop_traffic() -> dict:
    global _traffic_on
    with _traffic_lock:
        _traffic_on = False
    return {"ok": True, "running": False}


# ---- HTTP API ---------------------------------------------------------------------

app = FastAPI(title="Self-Healing Controller")


class EnableBody(BaseModel):
    enabled: bool = True


class CrashBody(BaseModel):
    slot_id: Optional[str] = None
    service: Optional[str] = None
    mode: str = Field(default="stop", description="stop | kill")


@app.on_event("startup")
def on_startup() -> None:
    load_state()
    try:
        bind_seed_containers()
    except Exception as exc:  # noqa: BLE001
        log_event("info", f"seed bind deferred: {exc}")
    threading.Thread(target=control_loop, name="heal-loop", daemon=True).start()
    log_event(
        "info",
        f"healer_start  enabled={_state['enabled']}  desired={_state['desired']}  "
        f"network={PROJECT_NETWORK}",
    )


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop.set()
    stop_traffic()


@app.get("/health")
def health():
    return {"status": "ok", "service": "healer", "enabled": _state["enabled"]}


@app.get("/status")
def status():
    with _lock:
        slots = {k: [dict(s) for s in v] for k, v in _state["slots"].items()}
        enabled = _state["enabled"]
        desired = dict(_state["desired"])
        pending = _state["pending"]
        replacing = _state["replacing"]
        events = list(_events)[-80:]
    summary = {}
    for shard, rows in slots.items():
        healthy = sum(1 for s in rows if s.get("status") == "healthy")
        summary[shard] = {
            "desired": desired.get(shard, len(rows)),
            "configured": len(rows),
            "healthy": healthy,
            "unhealthy": len(rows) - healthy,
        }
    with _traffic_lock:
        traffic = {
            "running": _traffic_on,
            "stats": dict(_traffic_stats),
            "requests": list(_traffic_log)[-60:],
        }
    map_view = None
    try:
        raw = load_map()
        map_view = {
            "epoch": raw.get("epoch"),
            "shards": {
                sid: {"replicas": list(meta.get("replicas") or [])}
                for sid, meta in (raw.get("shards") or {}).items()
            },
        }
    except Exception:
        map_view = None
    return {
        "ok": True,
        "enabled": enabled,
        "replacing": replacing,
        "pending": pending,
        "desired": desired,
        "summary": summary,
        "slots": slots,
        "events": events,
        "traffic": traffic,
        "shard_map": map_view,
        "fail_threshold": FAIL_THRESHOLD,
        "poll_secs": POLL_SECS,
    }


@app.post("/enable")
def set_enabled(body: EnableBody):
    with _lock:
        _state["enabled"] = bool(body.enabled)
    save_state()
    log_event("info", f"healer enabled={body.enabled}")
    return {"ok": True, "enabled": body.enabled}


@app.post("/traffic/start")
def traffic_start():
    return start_traffic()


@app.post("/traffic/stop")
def traffic_stop():
    return stop_traffic()


@app.post("/inject/crash")
def inject_crash(body: CrashBody):
    """Stop a replica container so the healer must replace it. Healthy peers untouched."""
    target = None
    with _lock:
        for rows in _state["slots"].values():
            for s in rows:
                if body.slot_id and s["slot_id"] == body.slot_id:
                    target = dict(s)
                elif body.service and s.get("service") == body.service:
                    target = dict(s)
    if not target and body.service:
        c = docker_service_container(body.service)
        if c:
            names = [n.lstrip("/") for n in (c.get("Names") or [])]
            target = {
                "slot_id": body.service,
                "container_name": names[0] if names else c["Id"][:12],
                "url": f"http://{body.service}:8000",
            }
    if not target or not target.get("container_name"):
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": "target slot/container not found"},
        )
    name = target["container_name"]
    try:
        info = docker_inspect(name)
        cid = info["Id"]
        if body.mode == "kill":
            docker_api(f"/containers/{cid}/kill", method="POST")
            msg = f"killed {name}"
        else:
            docker_api(f"/containers/{cid}/stop?t=1", method="POST")
            msg = f"stopped {name}"
        # Accel fail detection + immediate UI status
        _fail_streak[target["slot_id"]] = FAIL_THRESHOLD
        with _lock:
            for rows in _state["slots"].values():
                for s in rows:
                    if s["slot_id"] == target["slot_id"]:
                        s["status"] = "unhealthy"
                        s["container_running"] = False
        log_event("phase", f"injected crash: {msg}", slot=target["slot_id"])
        return {"ok": True, "message": msg, "slot": target}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


@app.post("/reconcile")
def force_reconcile():
    try:
        reconcile_once()
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
