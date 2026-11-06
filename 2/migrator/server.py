"""
Bonus I: Live re-sharding controller.
Moves A1501–A2000 from Shard B -> Shard C with online CDC, fencing, epoch cutover,
crash-safe checkpoints, and verbose phase logs for terminal demos.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import psycopg
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8095"))
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://bank:bank@db-coord:5432/coord"
)
SHARD_MAP_PATH = Path(os.getenv("SHARD_MAP_PATH", "/config/shard_map.json"))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8080").rstrip("/")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8090").rstrip("/")

SOURCE_SHARD = os.getenv("SOURCE_SHARD", "B")
TARGET_SHARD = os.getenv("TARGET_SHARD", "C")
RANGE_START = int(os.getenv("RANGE_START", "1501"))
RANGE_END = int(os.getenv("RANGE_END", "2000"))
SOURCE_KEEP_END = RANGE_START - 1  # B keeps 1001–1500

SOURCE_REPLICAS = [
    u.strip()
    for u in os.getenv(
        "SOURCE_REPLICAS", "http://bank-b1:8000,http://bank-b2:8000"
    ).split(",")
    if u.strip()
]
TARGET_REPLICAS = [
    u.strip()
    for u in os.getenv(
        "TARGET_REPLICAS", "http://bank-c1:8000,http://bank-c2:8000"
    ).split(",")
    if u.strip()
]

# Lab fault hooks: create these files under FAULT_DIR to pause a phase.
FAULT_DIR = Path(os.getenv("FAULT_DIR", "/faults"))

PHASES = [
    "INIT",
    "PROVISION_TARGET",
    "SNAPSHOT",
    "CATCHUP",
    "FENCE_SOURCE",
    "FINAL_CATCHUP",
    "WAIT_INFLIGHT_2PC",
    "ACTIVATE_MAP",
    "VERIFY",
    "CLEANUP_SOURCE",
    "DONE",
]

logger = logging.getLogger("migrator")
logger.setLevel(logging.INFO)
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)

_lock = threading.Lock()
_runner: Optional[threading.Thread] = None
_stop_requested = False
_events: list[dict[str, Any]] = []
_status: dict[str, Any] = {
    "running": False,
    "migration_id": None,
    "phase": None,
    "passed": None,
    "error": None,
    "stopped": False,
}


class MigrationStopped(Exception):
    """Operator requested a cooperative stop; job stays resumable."""


def log_event(level: str, message: str, **extra: Any) -> None:
    entry = {"t": time.time(), "level": level, "message": message, **extra}
    with _lock:
        _events.append(entry)
        if len(_events) > 500:
            del _events[: len(_events) - 500]
        _status["phase"] = extra.get("phase", _status.get("phase"))
    logger.info(f"{level.upper():5}  {message}")


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS migration_jobs (
                    migration_id TEXT PRIMARY KEY,
                    phase TEXT NOT NULL,
                    epoch_from INT NOT NULL,
                    epoch_to INT,
                    source_shard TEXT NOT NULL,
                    target_shard TEXT NOT NULL,
                    range_start INT NOT NULL,
                    range_end INT NOT NULL,
                    snapshot_pos BIGINT,
                    catchup_pos BIGINT,
                    checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    cutover_decided BOOLEAN NOT NULL DEFAULT FALSE,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        conn.commit()


def load_map() -> dict:
    return json.loads(SHARD_MAP_PATH.read_text(encoding="utf-8"))


def write_map(data: dict) -> None:
    """In-place write — required for Docker single-file bind mounts on Windows (no rename)."""
    text = json.dumps(data, indent=2) + "\n"
    with SHARD_MAP_PATH.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass


def http_json(
    method: str,
    url: str,
    payload: Optional[dict] = None,
    timeout: float = 30,
    retries: int = 8,
) -> dict:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.request(method, url, json=payload, timeout=timeout)
            try:
                data = r.json()
            except ValueError:
                data = {"status": "error", "message": r.text}
            if not isinstance(data, dict):
                data = {"status": "error", "message": str(data)}
            data["_http"] = r.status_code
            if r.status_code >= 400:
                raise RuntimeError(f"{method} {url} -> {r.status_code} {data}")
            return data
        except (requests.RequestException, RuntimeError) as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(min(0.4 * attempt, 2.0))
    raise RuntimeError(f"{method} {url} failed after {retries} tries: {last_exc}")


def source_url() -> str:
    return SOURCE_REPLICAS[0].rstrip("/")


def target_url() -> str:
    return TARGET_REPLICAS[0].rstrip("/")


def all_target_urls() -> list[str]:
    return [u.rstrip("/") for u in TARGET_REPLICAS]


def all_source_urls() -> list[str]:
    return [u.rstrip("/") for u in SOURCE_REPLICAS]


def wait_healthy(urls: list[str], timeout: float = 90) -> None:
    deadline = time.time() + timeout
    pending = set(urls)
    while pending and time.time() < deadline:
        raise_if_stopped()
        done = []
        for u in pending:
            try:
                r = requests.get(f"{u}/health", timeout=2)
                if r.status_code == 200 and r.json().get("status") == "ok":
                    done.append(u)
            except requests.RequestException:
                pass
        for u in done:
            pending.discard(u)
        if pending:
            time.sleep(1)
    if pending:
        raise RuntimeError(f"replicas not healthy: {sorted(pending)}")


def raise_if_stopped(phase: Optional[str] = None) -> None:
    with _lock:
        if _stop_requested:
            raise MigrationStopped(phase or _status.get("phase") or "?")


def fault_pause(name: str) -> None:
    flag = FAULT_DIR / name
    if not flag.exists():
        return
    log_event("info", f"fault pause armed: {name} (waiting until flag cleared)")
    while flag.exists():
        raise_if_stopped()
        time.sleep(0.3)
    log_event("info", f"fault pause cleared: {name}")


def save_job(migration_id: str, **fields: Any) -> None:
    sets = []
    vals = []
    for k, v in fields.items():
        if k == "checkpoint_json" and not isinstance(v, str):
            v = json.dumps(v)
            sets.append(f"{k} = %s::jsonb")
        else:
            sets.append(f"{k} = %s")
        vals.append(v)
    sets.append("updated_at = NOW()")
    vals.append(migration_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE migration_jobs SET {', '.join(sets)} WHERE migration_id = %s",
                tuple(vals),
            )
        conn.commit()


def get_job(migration_id: str) -> Optional[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migration_jobs WHERE migration_id = %s", (migration_id,)
            )
            return cur.fetchone()


def latest_open_job() -> Optional[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migration_jobs "
                "WHERE phase <> 'DONE' AND phase <> 'ABANDONED' "
                "ORDER BY updated_at DESC LIMIT 1"
            )
            return cur.fetchone()


def abandon_open_jobs(reason: str = "abandoned by operator") -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE migration_jobs SET phase='ABANDONED', error=%s, updated_at=NOW() "
                "WHERE phase <> 'DONE' AND phase <> 'ABANDONED'",
                (reason,),
            )
            n = cur.rowcount or 0
        conn.commit()
    return n


def set_ownership_all(urls: list[str], body: dict) -> None:
    for u in urls:
        http_json("POST", f"{u}/admin/ownership", body)


def inflight_migrating_txs() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT transaction_id, status, from_account, to_account
                FROM distributed_transactions
                WHERE status = ANY(%s)
                  AND (
                    (SUBSTRING(from_account FROM 2)::INT BETWEEN %s AND %s)
                    OR (SUBSTRING(to_account FROM 2)::INT BETWEEN %s AND %s)
                  )
                ORDER BY updated_at
                """,
                (
                    [
                        "INITIATED",
                        "PREPARING",
                        "PREPARED",
                        "COMMIT_DECIDED",
                        "ABORT_DECIDED",
                    ],
                    RANGE_START,
                    RANGE_END,
                    RANGE_START,
                    RANGE_END,
                ),
            )
            return list(cur.fetchall())


def reload_routers() -> None:
    for base in (GATEWAY_URL, COORDINATOR_URL):
        try:
            http_json("POST", f"{base}/admin/reload-map", {})
        except Exception as exc:  # noqa: BLE001
            log_event("info", f"reload hint failed for {base}: {exc}")


def run_migration(migration_id: str, resume: bool = False) -> None:
    global _stop_requested
    with _lock:
        _stop_requested = False
        _status.update(
            {
                "running": True,
                "migration_id": migration_id,
                "passed": None,
                "error": None,
                "stopped": False,
            }
        )
        if not resume:
            _events.clear()

    try:
        job = get_job(migration_id)
        if not job:
            raise RuntimeError("migration job missing")
        phase = job["phase"]
        checkpoint = job["checkpoint_json"] or {}
        if isinstance(checkpoint, str):
            checkpoint = json.loads(checkpoint)

        log_event(
            "info",
            f"migration {migration_id[:8]}… resume={resume} starting_phase={phase}",
            phase=phase,
        )

        # --- PROVISION_TARGET ---
        raise_if_stopped(phase)
        if phase in {"INIT", "PROVISION_TARGET"}:
            save_job(migration_id, phase="PROVISION_TARGET")
            log_event(
                "phase",
                f"PROVISION_TARGET: wait for Shard {TARGET_SHARD} replicas",
                phase="PROVISION_TARGET",
            )
            fault_pause("pause_before_snapshot")
            wait_healthy(all_target_urls())
            set_ownership_all(
                all_target_urls(),
                {
                    "epoch": int(job["epoch_from"]),
                    "owned_start": RANGE_START,
                    "owned_end": RANGE_END,
                    "write_enabled": False,
                    "clear_fence": True,
                },
            )
            log_event(
                "pass",
                f"Shard {TARGET_SHARD} healthy; writes disabled until cutover",
                phase="PROVISION_TARGET",
            )
            phase = "SNAPSHOT"
            save_job(migration_id, phase=phase)

        # --- SNAPSHOT ---
        raise_if_stopped(phase)
        if phase == "SNAPSHOT":
            log_event(
                "phase",
                f"SNAPSHOT: consistent copy of A{RANGE_START:04d}–A{RANGE_END:04d} from {SOURCE_SHARD}",
                phase="SNAPSHOT",
            )
            fault_pause("pause_during_snapshot")
            snap = http_json(
                "POST",
                f"{source_url()}/admin/snapshot",
                {"start": RANGE_START, "end": RANGE_END},
                timeout=120,
            )
            accounts = snap["accounts"]
            change_pos = int(snap["change_pos"])
            log_event(
                "info",
                f"snapshot captured count={snap['count']} sum={snap['sum_balance']} "
                f"durable_change_pos={change_pos}",
                phase="SNAPSHOT",
            )
            # Chunk apply to target (idempotent upserts).
            chunk = 100
            applied = 0
            for i in range(0, len(accounts), chunk):
                raise_if_stopped("SNAPSHOT")
                part = accounts[i : i + chunk]
                for tu in all_target_urls():
                    http_json(
                        "POST",
                        f"{tu}/admin/apply_snapshot",
                        {"accounts": part},
                        timeout=60,
                    )
                applied += len(part)
                if i == 0 or applied == len(accounts) or applied % 200 == 0:
                    log_event(
                        "info",
                        f"snapshot apply progress {applied}/{len(accounts)} -> {TARGET_SHARD}",
                        phase="SNAPSHOT",
                    )
                checkpoint = {
                    "snapshot_count": len(accounts),
                    "snapshot_sum": snap["sum_balance"],
                    "applied": applied,
                }
                save_job(
                    migration_id,
                    snapshot_pos=change_pos,
                    catchup_pos=change_pos,
                    checkpoint_json=checkpoint,
                )
            log_event(
                "pass",
                f"SNAPSHOT done; catch-up will replay changelog after seq={change_pos}",
                phase="SNAPSHOT",
            )
            phase = "CATCHUP"
            save_job(migration_id, phase=phase, snapshot_pos=change_pos, catchup_pos=change_pos)

        # --- CATCHUP (online) ---
        raise_if_stopped(phase)
        if phase == "CATCHUP":
            log_event(
                "phase",
                "CATCHUP: replay ordered durable changes while source still serves traffic",
                phase="CATCHUP",
            )
            fault_pause("pause_during_catchup")
            job = get_job(migration_id)
            assert job
            pos = int(job["catchup_pos"] or job["snapshot_pos"] or 0)
            # Under live traffic the changelog never goes fully idle.
            # Warm catch-up for a bounded window; FINAL_CATCHUP after fence drains the rest.
            warm_secs = float(os.getenv("CATCHUP_WARM_SECS", "8"))
            deadline = time.time() + warm_secs
            batches = 0
            while time.time() < deadline:
                raise_if_stopped("CATCHUP")
                ch = http_json(
                    "POST",
                    f"{source_url()}/admin/changes",
                    {
                        "start": RANGE_START,
                        "end": RANGE_END,
                        "after_seq": pos,
                        "limit": 300,
                    },
                )
                changes = ch["changes"]
                if not changes:
                    lag = int(ch["max_seq"]) - pos
                    log_event(
                        "info",
                        f"catchup caught up to live tip pos={pos} lag={lag}",
                        phase="CATCHUP",
                    )
                    time.sleep(0.25)
                    continue
                for tu in all_target_urls():
                    http_json(
                        "POST",
                        f"{tu}/admin/apply_changes",
                        {"changes": changes},
                    )
                pos = int(changes[-1]["seq"])
                batches += 1
                save_job(migration_id, catchup_pos=pos)
                if batches <= 5 or batches % 5 == 0:
                    log_event(
                        "info",
                        f"catchup applied {len(changes)} changes -> pos={pos} "
                        f"(warm window)",
                        phase="CATCHUP",
                    )
            log_event(
                "pass",
                f"CATCHUP warm done after {warm_secs:.0f}s; last pos={pos}; "
                "remaining deltas drained after fence",
                phase="CATCHUP",
            )
            phase = "FENCE_SOURCE"
            save_job(migration_id, phase=phase)

        # --- FENCE_SOURCE ---
        raise_if_stopped(phase)
        if phase == "FENCE_SOURCE":
            log_event(
                "phase",
                "FENCE_SOURCE: brief write fence on old owner for migrating range",
                phase="FENCE_SOURCE",
            )
            fault_pause("pause_during_fencing")
            set_ownership_all(
                all_source_urls(),
                {
                    "fenced_start": RANGE_START,
                    "fenced_end": RANGE_END,
                },
            )
            log_event(
                "pass",
                f"Shard {SOURCE_SHARD} fenced writes for A{RANGE_START:04d}–A{RANGE_END:04d}",
                phase="FENCE_SOURCE",
            )
            phase = "FINAL_CATCHUP"
            save_job(migration_id, phase=phase)

        # --- FINAL_CATCHUP ---
        raise_if_stopped(phase)
        if phase == "FINAL_CATCHUP":
            log_event(
                "phase",
                "FINAL_CATCHUP: drain remaining changelog after fence",
                phase="FINAL_CATCHUP",
            )
            job = get_job(migration_id)
            assert job
            pos = int(job["catchup_pos"] or 0)
            for _ in range(50):
                raise_if_stopped("FINAL_CATCHUP")
                ch = http_json(
                    "POST",
                    f"{source_url()}/admin/changes",
                    {
                        "start": RANGE_START,
                        "end": RANGE_END,
                        "after_seq": pos,
                        "limit": 500,
                    },
                )
                changes = ch["changes"]
                if not changes:
                    break
                for tu in all_target_urls():
                    http_json(
                        "POST",
                        f"{tu}/admin/apply_changes",
                        {"changes": changes},
                    )
                pos = int(changes[-1]["seq"])
                save_job(migration_id, catchup_pos=pos)
                log_event(
                    "info",
                    f"final catchup +{len(changes)} -> pos={pos}",
                    phase="FINAL_CATCHUP",
                )
            log_event("pass", f"final catchup complete at pos={pos}", phase="FINAL_CATCHUP")
            phase = "WAIT_INFLIGHT_2PC"
            save_job(migration_id, phase=phase)

        # --- WAIT_INFLIGHT_2PC ---
        raise_if_stopped(phase)
        if phase == "WAIT_INFLIGHT_2PC":
            log_event(
                "phase",
                "WAIT_INFLIGHT_2PC: resolve/block prepared txs touching migrating accounts",
                phase="WAIT_INFLIGHT_2PC",
            )
            deadline = time.time() + 60
            while time.time() < deadline:
                raise_if_stopped("WAIT_INFLIGHT_2PC")
                inflight = inflight_migrating_txs()
                if not inflight:
                    break
                log_event(
                    "info",
                    f"waiting for {len(inflight)} in-flight 2PC txs: "
                    + ", ".join(
                        f"{r['transaction_id'][:8]}:{r['status']}" for r in inflight[:5]
                    ),
                    phase="WAIT_INFLIGHT_2PC",
                )
                time.sleep(1)
            left = inflight_migrating_txs()
            if left:
                raise RuntimeError(
                    f"in-flight 2PC still open after wait: "
                    f"{[(x['transaction_id'][:8], x['status']) for x in left[:5]]}"
                )
            log_event("pass", "no in-flight 2PC on migrating accounts", phase="WAIT_INFLIGHT_2PC")
            phase = "ACTIVATE_MAP"
            save_job(migration_id, phase=phase)

        # --- ACTIVATE_MAP ---
        raise_if_stopped(phase)
        if phase == "ACTIVATE_MAP":
            log_event(
                "phase",
                "ACTIVATE_MAP: durable cutover decision + epoch bump + shard map publish",
                phase="ACTIVATE_MAP",
            )
            fault_pause("pause_before_activate")
            job = get_job(migration_id)
            assert job
            epoch_from = int(job["epoch_from"])
            epoch_to = epoch_from + 1
            save_job(
                migration_id,
                cutover_decided=True,
                epoch_to=epoch_to,
            )
            log_event(
                "info",
                f"cutover decision durable: epoch {epoch_from} -> {epoch_to}",
                phase="ACTIVATE_MAP",
            )

            # Enable target as owner at new epoch; shrink source ownership.
            set_ownership_all(
                all_target_urls(),
                {
                    "epoch": epoch_to,
                    "owned_start": RANGE_START,
                    "owned_end": RANGE_END,
                    "write_enabled": True,
                    "clear_fence": True,
                },
            )
            set_ownership_all(
                all_source_urls(),
                {
                    "epoch": epoch_to,
                    "owned_start": 1001,
                    "owned_end": SOURCE_KEEP_END,
                    "write_enabled": True,
                    "clear_fence": True,
                },
            )

            raw = load_map()
            raw["epoch"] = epoch_to
            raw["shards"][SOURCE_SHARD]["start"] = 1001
            raw["shards"][SOURCE_SHARD]["end"] = SOURCE_KEEP_END
            raw["shards"][TARGET_SHARD] = {
                "start": RANGE_START,
                "end": RANGE_END,
                "replicas": TARGET_REPLICAS,
            }
            write_map(raw)
            reload_routers()
            time.sleep(1.2)
            reload_routers()
            log_event(
                "pass",
                f"map activated epoch={epoch_to}: "
                f"{SOURCE_SHARD}=1001-{SOURCE_KEEP_END}, "
                f"{TARGET_SHARD}={RANGE_START}-{RANGE_END}",
                phase="ACTIVATE_MAP",
            )
            phase = "VERIFY"
            save_job(migration_id, phase=phase, epoch_to=epoch_to)

        # --- VERIFY ---
        raise_if_stopped(phase)
        if phase == "VERIFY":
            log_event(
                "phase",
                "VERIFY: target has range; gateway routes to C; stale B writes fail",
                phase="VERIFY",
            )
            src_stats = http_json(
                "POST",
                f"{source_url()}/admin/range_stats",
                {"start": RANGE_START, "end": RANGE_END},
            )
            dst_stats = http_json(
                "POST",
                f"{target_url()}/admin/range_stats",
                {"start": RANGE_START, "end": RANGE_END},
            )
            log_event(
                "info",
                f"range stats source_count={src_stats['count']} "
                f"target_count={dst_stats['count']} target_sum={dst_stats['sum_balance']}",
                phase="VERIFY",
            )
            if int(dst_stats["count"]) <= 0:
                raise RuntimeError("target shard has no migrated accounts")
            sm = http_json("GET", f"{GATEWAY_URL}/shard-map")
            if "C" not in sm.get("shards", {}):
                raise RuntimeError("gateway shard-map missing shard C")
            if int(sm["shards"]["C"]["start"]) != RANGE_START:
                raise RuntimeError("gateway shard C range incorrect")
            log_event(
                "pass",
                f"gateway epoch={sm['epoch']} routes A{RANGE_START:04d}+ to C",
                phase="VERIFY",
            )
            phase = "CLEANUP_SOURCE"
            save_job(migration_id, phase=phase)

        # --- CLEANUP_SOURCE ---
        raise_if_stopped(phase)
        if phase == "CLEANUP_SOURCE":
            log_event(
                "phase",
                "CLEANUP_SOURCE: delete old range from B only after verified cutover",
                phase="CLEANUP_SOURCE",
            )
            fault_pause("pause_before_cleanup")
            # Only cleanup via one replica (shared DB).
            deleted = http_json(
                "POST",
                f"{source_url()}/admin/delete_range",
                {"start": RANGE_START, "end": RANGE_END},
            )
            src_stats = http_json(
                "POST",
                f"{source_url()}/admin/range_stats",
                {"start": RANGE_START, "end": RANGE_END},
            )
            if int(src_stats["count"]) != 0:
                raise RuntimeError("source still holds migrated accounts after cleanup")
            log_event(
                "pass",
                f"old owner cleaned deleted={deleted.get('deleted')} remaining={src_stats['count']}",
                phase="CLEANUP_SOURCE",
            )
            phase = "DONE"
            save_job(migration_id, phase=phase)

        if phase == "DONE":
            log_event(
                "pass",
                "DONE: live re-shard complete without dual writers",
                phase="DONE",
            )
            with _lock:
                _status["passed"] = True
                _status["running"] = False
            return

        raise RuntimeError(f"unhandled phase {phase}")
    except MigrationStopped as exc:
        stopped_phase = str(exc.args[0]) if exc.args else (_status.get("phase") or "?")
        log_event(
            "info",
            f"migration stopped by operator at phase={stopped_phase} (resumable)",
            phase=stopped_phase,
        )
        with _lock:
            _stop_requested = False
            _status["passed"] = None
            _status["error"] = None
            _status["stopped"] = True
            _status["running"] = False
            _status["phase"] = stopped_phase
    except Exception as exc:  # noqa: BLE001
        log_event("fail", f"migration failed: {exc}", phase=_status.get("phase"))
        try:
            save_job(migration_id, error=str(exc))
        except Exception:  # noqa: BLE001
            pass
        with _lock:
            _stop_requested = False
            _status["passed"] = False
            _status["error"] = str(exc)
            _status["stopped"] = False
            _status["running"] = False
        raise


def start_locked(
    resume_id: Optional[str] = None,
    force_new: bool = False,
) -> dict:
    global _runner, _stop_requested
    with _lock:
        if _status["running"]:
            return {
                "ok": False,
                "error": f"migration {_status['migration_id']} already running",
            }
        if _runner and _runner.is_alive():
            return {"ok": False, "error": "runner thread still alive"}
        _stop_requested = False
        _status["stopped"] = False

    ensure_schema()
    if force_new and not resume_id:
        n = abandon_open_jobs("superseded by force_new start")
        if n:
            log_event("info", f"abandoned {n} previous open migration job(s)")

    if resume_id:
        job = get_job(resume_id)
        if not job:
            return {"ok": False, "error": "unknown migration_id"}
        migration_id = resume_id
        resume = True
    else:
        open_job = None if force_new else latest_open_job()
        if open_job and open_job["phase"] not in {"DONE", "ABANDONED"}:
            migration_id = open_job["migration_id"]
            resume = True
            log_event(
                "info",
                f"auto-resuming open migration {migration_id[:8]}… phase={open_job['phase']}",
            )
        else:
            raw = load_map()
            migration_id = uuid.uuid4().hex
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO migration_jobs "
                        "(migration_id, phase, epoch_from, source_shard, target_shard, "
                        "range_start, range_end) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (
                            migration_id,
                            "INIT",
                            int(raw.get("epoch", 1)),
                            SOURCE_SHARD,
                            TARGET_SHARD,
                            RANGE_START,
                            RANGE_END,
                        ),
                    )
                conn.commit()
            resume = False

    def _target() -> None:
        try:
            run_migration(migration_id, resume=resume)
        except Exception:
            pass

    _runner = threading.Thread(target=_target, daemon=True, name="migration-runner")
    _runner.start()
    return {
        "ok": True,
        "migration_id": migration_id,
        "resume": resume,
    }


app = FastAPI(title="Live Re-Shard Migrator")


class StartBody(BaseModel):
    resume_migration_id: Optional[str] = None
    force_new: bool = False


@app.on_event("startup")
def on_startup() -> None:
    ensure_schema()
    FAULT_DIR.mkdir(parents=True, exist_ok=True)
    # Crash recovery: if a non-DONE job exists, continue automatically.
    open_job = latest_open_job()
    if open_job and open_job["phase"] not in {"DONE", "ABANDONED"}:
        log_event(
            "info",
            f"startup recovery for migration {open_job['migration_id'][:8]}… "
            f"phase={open_job['phase']}",
        )
        start_locked(resume_id=open_job["migration_id"])
    logger.info(f"migrator_start  port={PORT}")


@app.get("/health")
def health():
    return {"status": "ok", "service": "migrator"}


@app.get("/status")
def status():
    with _lock:
        return {
            **_status,
            "events": list(_events),
            "job": get_job(_status["migration_id"]) if _status.get("migration_id") else latest_open_job(),
        }


@app.post("/migrate/start")
def migrate_start(body: StartBody = StartBody()):
    result = start_locked(
        resume_id=body.resume_migration_id,
        force_new=body.force_new,
    )
    if not result.get("ok"):
        return JSONResponse(status_code=409, content=result)
    return result


@app.post("/migrate/abandon")
def migrate_abandon():
    global _stop_requested
    with _lock:
        _stop_requested = True
    n = abandon_open_jobs("abandoned via API")
    return {"ok": True, "abandoned": n}


@app.post("/migrate/stop")
def migrate_stop():
    """Cooperative stop: runner exits at the next safe checkpoint; job stays open."""
    global _stop_requested
    with _lock:
        if not _status["running"]:
            return {
                "ok": True,
                "stopped": False,
                "message": "no migration running",
                "migration_id": _status.get("migration_id"),
                "phase": _status.get("phase"),
            }
        _stop_requested = True
        mid = _status.get("migration_id")
        phase = _status.get("phase")
    log_event("info", f"stop requested for migration {(mid or '')[:8]}… phase={phase}")
    return {
        "ok": True,
        "stopped": True,
        "message": "stop requested; waiting for runner to pause",
        "migration_id": mid,
        "phase": phase,
    }


@app.post("/migrate/resume")
def migrate_resume():
    open_job = latest_open_job()
    if not open_job:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "no open migration"}
        )
    result = start_locked(resume_id=open_job["migration_id"])
    if not result.get("ok"):
        return JSONResponse(status_code=409, content=result)
    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
