"""Bonus I live dashboard: traffic generator + aggregated snapshot for UI."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import psycopg
import requests
from psycopg.rows import dict_row

GATEWAY_URL = "http://gateway:8080"
COORDINATOR_URL = "http://coordinator:8090"
MIGRATOR_URL = "http://migrator:8095"
DB_A_URL = ""
DB_B_URL = ""
DB_C_URL = ""
DB_COORD_URL = ""
BANK_B_ADMIN = "http://bank-b1:8000"
BANK_C_ADMIN = "http://bank-c1:8000"
SHARD_MAP_PATH = Path(os.getenv("SHARD_MAP_PATH", "/config/shard_map.json"))

WATCH = [
    "A0500",
    "A1000",
    "A1200",
    "A1500",
    "A1600",
    "A1800",
    "A2000",
]

_lock = threading.Lock()
_traffic_on = False
_traffic_thread: Optional[threading.Thread] = None
_requests: list[dict[str, Any]] = []
_stats = {"sent": 0, "ok": 0, "fail": 0, "fenced": 0}


def configure(
    *,
    gateway_url: str,
    coordinator_url: str,
    migrator_url: str,
    db_a_url: str,
    db_b_url: str,
    db_c_url: str,
    db_coord_url: str,
    bank_b_admin: str,
    bank_c_admin: str,
) -> None:
    global GATEWAY_URL, COORDINATOR_URL, MIGRATOR_URL
    global DB_A_URL, DB_B_URL, DB_C_URL, DB_COORD_URL, BANK_B_ADMIN, BANK_C_ADMIN
    GATEWAY_URL = gateway_url.rstrip("/")
    COORDINATOR_URL = coordinator_url.rstrip("/")
    MIGRATOR_URL = migrator_url.rstrip("/")
    DB_A_URL = db_a_url
    DB_B_URL = db_b_url
    DB_C_URL = db_c_url
    DB_COORD_URL = db_coord_url
    BANK_B_ADMIN = bank_b_admin.rstrip("/")
    BANK_C_ADMIN = bank_c_admin.rstrip("/")


def _push_req(row: dict[str, Any]) -> None:
    with _lock:
        _requests.append(row)
        if len(_requests) > 300:
            del _requests[: len(_requests) - 300]
        _stats["sent"] += 1
        if row.get("ok"):
            _stats["ok"] += 1
        else:
            _stats["fail"] += 1
        msg = str(row.get("detail") or "").lower()
        if "fence" in msg or "stale" in msg:
            _stats["fenced"] += 1


def _gw(payload: dict, timeout: float = 20) -> tuple[int, dict]:
    r = requests.post(f"{GATEWAY_URL}/transaction", json=payload, timeout=timeout)
    try:
        data = r.json()
    except ValueError:
        data = {"status": "error", "message": r.text}
    if not isinstance(data, dict):
        data = {"status": "error", "message": str(data)}
    return r.status_code, data


def _traffic_loop() -> None:
    i = 0
    while True:
        with _lock:
            if not _traffic_on:
                return
        i += 1
        jobs = [
            ("deposit", {"action": "deposit", "account": "A1600", "amount": 1}),
            ("deposit", {"action": "deposit", "account": "A1200", "amount": 1}),
            ("deposit", {"action": "deposit", "account": "A0500", "amount": 1}),
            ("withdraw", {"action": "withdraw", "account": "A1600", "amount": 1}),
        ]
        if i % 2 == 0:
            jobs.append(
                (
                    "transfer",
                    {
                        "action": "transfer",
                        "account": "A0500",
                        "to_account": "A1800",
                        "amount": 1,
                    },
                )
            )
        if i % 3 == 0:
            jobs.append(
                (
                    "transfer",
                    {
                        "action": "transfer",
                        "account": "A1800",
                        "to_account": "A0500",
                        "amount": 1,
                    },
                )
            )

        for kind, payload in jobs:
            with _lock:
                if not _traffic_on:
                    return
            payload = dict(payload)
            payload["client_request_id"] = uuid.uuid4().hex
            t0 = time.time()
            try:
                code, data = _gw(payload, timeout=25)
                ms = int((time.time() - t0) * 1000)
                ok = code == 200 and (
                    data.get("status") == "success" or data.get("decision") == "COMMIT"
                )
                pending = str(data.get("status", "")).upper() == "PENDING"
                _push_req(
                    {
                        "t": time.time(),
                        "kind": kind,
                        "account": payload.get("account"),
                        "to_account": payload.get("to_account"),
                        "amount": payload.get("amount"),
                        "ok": ok or pending,
                        "pending": pending,
                        "ms": ms,
                        "shard": data.get("shard_id") or data.get("mode"),
                        "replica": data.get("replica_id"),
                        "tx": (data.get("transaction_id") or "")[:8] or None,
                        "decision": data.get("decision"),
                        "detail": data.get("message")
                        or data.get("status")
                        or data.get("decision")
                        or "",
                        "http": code,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                _push_req(
                    {
                        "t": time.time(),
                        "kind": kind,
                        "account": payload.get("account"),
                        "to_account": payload.get("to_account"),
                        "amount": payload.get("amount"),
                        "ok": False,
                        "pending": False,
                        "ms": int((time.time() - t0) * 1000),
                        "detail": str(exc),
                        "http": 0,
                    }
                )
        # Keep near real-time; no artificial slowdown beyond tiny yield.
        time.sleep(0.05)


def start_traffic() -> dict:
    global _traffic_on, _traffic_thread
    with _lock:
        if _traffic_on:
            return {"ok": True, "running": True, "message": "already running"}
        _traffic_on = True
        _traffic_thread = threading.Thread(
            target=_traffic_loop, daemon=True, name="bonus-traffic"
        )
        _traffic_thread.start()
    return {"ok": True, "running": True}


def stop_traffic() -> dict:
    global _traffic_on
    with _lock:
        _traffic_on = False
    return {"ok": True, "running": False}


def clear_logs() -> dict:
    with _lock:
        _requests.clear()
        _stats.update({"sent": 0, "ok": 0, "fail": 0, "fenced": 0})
    return {"ok": True, "cleared": True}


def traffic_status() -> dict:
    with _lock:
        return {
            "running": _traffic_on,
            "stats": dict(_stats),
            "requests": list(_requests)[-120:],
        }


def _fetch_accounts(url: str, ids: list[str]) -> dict[str, Any]:
    try:
        with psycopg.connect(url, row_factory=dict_row, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT account_id, balance FROM accounts "
                    "WHERE account_id = ANY(%s) ORDER BY account_id",
                    (ids,),
                )
                rows = [
                    {
                        "account_id": r["account_id"],
                        "balance": float(Decimal(r["balance"])),
                    }
                    for r in cur.fetchall()
                ]
                cur.execute("SELECT COUNT(*) AS n, COALESCE(SUM(balance),0) AS s FROM accounts")
                tot = cur.fetchone()
                # Range counts for migration visibility
                cur.execute(
                    "SELECT COUNT(*) AS n, COALESCE(SUM(balance),0) AS s FROM accounts "
                    "WHERE SUBSTRING(account_id FROM 2)::INT BETWEEN 1501 AND 2000"
                )
                mig = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) AS n, COALESCE(SUM(balance),0) AS s FROM accounts "
                    "WHERE SUBSTRING(account_id FROM 2)::INT BETWEEN 1001 AND 1500"
                )
                low = cur.fetchone()
                own = None
                try:
                    cur.execute("SELECT * FROM ownership_state WHERE id=1")
                    own = cur.fetchone()
                    if own:
                        own = dict(own)
                        own["updated_at"] = (
                            own["updated_at"].isoformat() if own.get("updated_at") else None
                        )
                except Exception:  # noqa: BLE001
                    own = None
                changes = []
                try:
                    cur.execute(
                        "SELECT seq, account_id, balance, op, created_at "
                        "FROM account_changelog ORDER BY seq DESC LIMIT 25"
                    )
                    for r in cur.fetchall():
                        changes.append(
                            {
                                "seq": int(r["seq"]),
                                "account_id": r["account_id"],
                                "balance": float(Decimal(r["balance"])),
                                "op": r["op"],
                                "created_at": r["created_at"].isoformat()
                                if r.get("created_at")
                                else None,
                            }
                        )
                except Exception:  # noqa: BLE001
                    changes = []
        return {
            "ok": True,
            "accounts": rows,
            "total_count": int(tot["n"]),
            "total_sum": float(Decimal(tot["s"])),
            "range_1501_2000": {
                "count": int(mig["n"]),
                "sum": float(Decimal(mig["s"])),
            },
            "range_1001_1500": {
                "count": int(low["n"]),
                "sum": float(Decimal(low["s"])),
            },
            "ownership": own,
            "changelog": changes,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": str(exc),
            "accounts": [],
            "total_count": 0,
            "total_sum": 0,
            "range_1501_2000": {"count": 0, "sum": 0},
            "range_1001_1500": {"count": 0, "sum": 0},
            "ownership": None,
            "changelog": [],
        }


def _admin_ownership(base: str) -> dict:
    try:
        r = requests.get(f"{base}/admin/ownership", timeout=2)
        if r.status_code >= 400:
            return {"ok": False, "error": r.text}
        data = r.json()
        data["ok"] = True
        return data
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc)}


def _migrator_status() -> dict:
    try:
        r = requests.get(f"{MIGRATOR_URL}/status", timeout=3)
        return r.json()
    except requests.RequestException as exc:
        return {"running": False, "error": str(exc), "events": [], "job": None}


def _inflight_2pc() -> list[dict]:
    try:
        with psycopg.connect(DB_COORD_URL, row_factory=dict_row, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT transaction_id, from_account, to_account, amount, status,
                           from_shard, to_shard, updated_at
                    FROM distributed_transactions
                    WHERE status = ANY(%s)
                      AND (
                        (SUBSTRING(from_account FROM 2)::INT BETWEEN 1501 AND 2000)
                        OR (SUBSTRING(to_account FROM 2)::INT BETWEEN 1501 AND 2000)
                      )
                    ORDER BY updated_at DESC
                    LIMIT 30
                    """,
                    (
                        [
                            "INITIATED",
                            "PREPARING",
                            "PREPARED",
                            "COMMIT_DECIDED",
                            "ABORT_DECIDED",
                        ],
                    ),
                )
                rows = []
                for r in cur.fetchall():
                    rows.append(
                        {
                            "transaction_id": r["transaction_id"],
                            "from_account": r["from_account"],
                            "to_account": r["to_account"],
                            "amount": float(r["amount"]),
                            "status": r["status"],
                            "from_shard": r["from_shard"],
                            "to_shard": r["to_shard"],
                            "updated_at": r["updated_at"].isoformat()
                            if r.get("updated_at")
                            else None,
                        }
                    )
                return rows
    except Exception:  # noqa: BLE001
        return []


def _recent_2pc() -> list[dict]:
    try:
        with psycopg.connect(DB_COORD_URL, row_factory=dict_row, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT transaction_id, from_account, to_account, amount, status,
                           from_shard, to_shard, updated_at
                    FROM distributed_transactions
                    WHERE (
                        (SUBSTRING(from_account FROM 2)::INT BETWEEN 1501 AND 2000)
                        OR (SUBSTRING(to_account FROM 2)::INT BETWEEN 1501 AND 2000)
                    )
                    ORDER BY updated_at DESC
                    LIMIT 20
                    """
                )
                out = []
                for r in cur.fetchall():
                    out.append(
                        {
                            "transaction_id": r["transaction_id"],
                            "from_account": r["from_account"],
                            "to_account": r["to_account"],
                            "amount": float(r["amount"]),
                            "status": r["status"],
                            "from_shard": r["from_shard"],
                            "to_shard": r["to_shard"],
                            "updated_at": r["updated_at"].isoformat()
                            if r.get("updated_at")
                            else None,
                        }
                    )
                return out
    except Exception:  # noqa: BLE001
        return []


def _shard_map() -> dict:
    try:
        return requests.get(f"{GATEWAY_URL}/shard-map", timeout=2).json()
    except requests.RequestException as exc:
        return {"error": str(exc), "epoch": None, "shards": {}}


def snapshot() -> dict[str, Any]:
    db_a = _fetch_accounts(DB_A_URL, [a for a in WATCH if a <= "A1000"])
    db_b = _fetch_accounts(DB_B_URL, WATCH)
    db_c = _fetch_accounts(DB_C_URL, WATCH)
    return {
        "ts": time.time(),
        "traffic": traffic_status(),
        "migrator": _migrator_status(),
        "shard_map": _shard_map(),
        "ownership": {
            "B": _admin_ownership(BANK_B_ADMIN),
            "C": _admin_ownership(BANK_C_ADMIN),
        },
        "databases": {"A": db_a, "B": db_b, "C": db_c},
        "inflight_2pc": _inflight_2pc(),
        "recent_2pc": _recent_2pc(),
    }


def start_migration(force_new: bool = True) -> dict:
    try:
        if force_new:
            requests.post(f"{MIGRATOR_URL}/migrate/abandon", timeout=5)
        r = requests.post(
            f"{MIGRATOR_URL}/migrate/start",
            json={"force_new": force_new},
            timeout=10,
        )
        return r.json()
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc)}


def stop_migration() -> dict:
    try:
        r = requests.post(f"{MIGRATOR_URL}/migrate/stop", timeout=10)
        return r.json()
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc)}


def resume_migration() -> dict:
    try:
        r = requests.post(f"{MIGRATOR_URL}/migrate/resume", timeout=10)
        try:
            body = r.json()
        except ValueError:
            body = {"ok": False, "error": r.text}
        if r.status_code >= 400:
            body.setdefault("ok", False)
        return body
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc)}


def _write_initial_shard_map() -> None:
    data = {
        "epoch": 1,
        "shards": {
            "A": {
                "start": 1,
                "end": 1000,
                "replicas": ["http://bank-a1:8000", "http://bank-a2:8000"],
            },
            "B": {
                "start": 1001,
                "end": 2000,
                "replicas": ["http://bank-b1:8000", "http://bank-b2:8000"],
            },
        },
    }
    text = json.dumps(data, indent=2) + "\n"
    SHARD_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SHARD_MAP_PATH.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass


def _exec_sql(url: str, sql: str) -> None:
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def _set_ownership(base: str, body: dict) -> dict:
    try:
        r = requests.post(f"{base}/admin/ownership", json=body, timeout=8)
        return {"ok": r.status_code < 400, "status": r.status_code, "body": r.json() if r.content else {}}
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc)}


def reset_demo_state() -> dict:
    """Stop traffic, clear logs, restore B/C + shard map to pre-migration state."""
    notes: list[str] = []
    stop_traffic()
    clear_logs()

    try:
        r = requests.post(f"{MIGRATOR_URL}/migrate/abandon", timeout=5)
        notes.append(f"migrator abandon: {r.status_code}")
    except requests.RequestException as exc:
        notes.append(f"migrator abandon skipped: {exc}")

    try:
        _write_initial_shard_map()
        notes.append("shard_map reset to epoch=1 (A+B)")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"shard_map write failed: {exc}", "notes": notes}

    for base in (GATEWAY_URL, COORDINATOR_URL):
        try:
            requests.post(f"{base}/admin/reload-map", timeout=5)
        except requests.RequestException as exc:
            notes.append(f"reload {base}: {exc}")

    # Wipe CDC / request noise on A (balances stay — A is not in the migrate range).
    try:
        _exec_sql(
            DB_A_URL,
            """
            TRUNCATE account_changelog RESTART IDENTITY;
            TRUNCATE applied_changes;
            TRUNCATE client_requests;
            """,
        )
        notes.append("DB A: changelog cleared")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"DB A reset failed: {exc}", "notes": notes}

    # Restore migrating range on B and wipe changelog noise.
    try:
        _exec_sql(
            DB_B_URL,
            """
            INSERT INTO accounts(account_id, balance)
            SELECT 'A' || lpad(i::text, 4, '0'), 1000.00
            FROM generate_series(1501, 2000) AS i
            ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance;
            TRUNCATE account_changelog RESTART IDENTITY;
            TRUNCATE applied_changes;
            TRUNCATE client_requests;
            """,
        )
        notes.append("DB B: A1501–A2000 restored to 1000.00")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"DB B reset failed: {exc}", "notes": notes}

    # Empty C completely for a clean target.
    try:
        _exec_sql(
            DB_C_URL,
            """
            TRUNCATE accounts RESTART IDENTITY CASCADE;
            TRUNCATE account_changelog RESTART IDENTITY;
            TRUNCATE applied_changes;
            TRUNCATE client_requests;
            """,
        )
        notes.append("DB C: wiped to empty")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"DB C reset failed: {exc}", "notes": notes}

    b_own = _set_ownership(
        BANK_B_ADMIN,
        {
            "epoch": 1,
            "owned_start": 1001,
            "owned_end": 2000,
            "write_enabled": True,
            "clear_fence": True,
        },
    )
    c_own = _set_ownership(
        BANK_C_ADMIN,
        {
            "epoch": 1,
            "owned_start": 1501,
            "owned_end": 2000,
            "write_enabled": False,
            "clear_fence": True,
        },
    )
    notes.append(f"ownership B: {b_own}")
    notes.append(f"ownership C: {c_own}")

    # Reload maps again after ownership restore.
    for base in (GATEWAY_URL, COORDINATOR_URL):
        try:
            requests.post(f"{base}/admin/reload-map", timeout=5)
        except requests.RequestException:
            pass

    return {
        "ok": bool(b_own.get("ok")) and bool(c_own.get("ok")),
        "notes": notes,
        "ownership_b": b_own,
        "ownership_c": c_own,
    }
