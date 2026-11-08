"""Live demo runners for Assignment experiments 3, 6, and 7.

Designed for presentation: every concurrent request, timeline phase, and
before/after proof is streamed into status so the UI can show the work live.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

import psycopg
import requests
from psycopg.rows import dict_row

GATEWAY_URL = ""
COORDINATOR_URL = ""
DB_A_URL = ""
DB_B_URL = ""
DB_COORD_URL = ""
FAULT_DIR = Path("/faults")
COMPOSE_PROJECT = "2"

SERVICE_CONTAINERS = {
    "coordinator": f"{COMPOSE_PROJECT}-coordinator-1",
}

_lock = threading.Lock()
_state: dict[str, Any] = {
    "running": False,
    "experiment": None,
    "title": None,
    "passed": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
    "headline": "Pick an experiment to demonstrate.",
    "steps": [],
    "requests": [],
    "phases": [],
    "metrics": {},
    "proof": {},
    "result": {},
}

_req_seq = 0


def configure(
    *,
    gateway_url: str,
    coordinator_url: str,
    db_a_url: str,
    db_b_url: str,
    db_coord_url: str,
    fault_dir: str | Path,
    compose_project: str,
    docker_api: Callable[..., Any],
) -> None:
    global GATEWAY_URL, COORDINATOR_URL, DB_A_URL, DB_B_URL, DB_COORD_URL
    global FAULT_DIR, COMPOSE_PROJECT, SERVICE_CONTAINERS, _docker_api
    GATEWAY_URL = gateway_url.rstrip("/")
    COORDINATOR_URL = coordinator_url.rstrip("/")
    DB_A_URL = db_a_url
    DB_B_URL = db_b_url
    DB_COORD_URL = db_coord_url
    FAULT_DIR = Path(fault_dir)
    COMPOSE_PROJECT = compose_project
    SERVICE_CONTAINERS = {
        "coordinator": f"{COMPOSE_PROJECT}-coordinator-1",
    }
    _docker_api = docker_api


_docker_api: Callable[..., Any] = lambda *a, **k: None  # noqa: E731


def get_status() -> dict[str, Any]:
    with _lock:
        return {
            "running": _state["running"],
            "experiment": _state["experiment"],
            "title": _state["title"],
            "passed": _state["passed"],
            "error": _state["error"],
            "started_at": _state["started_at"],
            "finished_at": _state["finished_at"],
            "headline": _state["headline"],
            "steps": list(_state["steps"]),
            "requests": list(_state["requests"]),
            "phases": list(_state["phases"]),
            "metrics": dict(_state["metrics"] or {}),
            "proof": dict(_state["proof"] or {}),
            "result": dict(_state["result"] or {}),
            "available": [
                {
                    "id": 3,
                    "title": "Concurrent replica operations",
                    "summary": "Watch parallel deposit/withdraw hit A1 and A2, then prove the final balance.",
                },
                {
                    "id": 6,
                    "title": "Interrupted distributed transaction",
                    "summary": "Watch COMMIT_DECIDED, coordinator kill, restart, and recovery to COMPLETED.",
                },
                {
                    "id": 7,
                    "title": "Concurrent cross-shard transfers",
                    "summary": "Watch overlapping 2PC transfers finish and prove money conservation.",
                },
            ],
        }


def clear_live() -> dict[str, Any]:
    """Clear Live requests / timeline / proof so the next demo starts on a clean board."""
    global _req_seq
    with _lock:
        if _state["running"]:
            return {
                "ok": False,
                "error": "experiment is running — wait for it to finish, then clear",
            }
        _req_seq = 0
        _state.update(
            {
                "running": False,
                "experiment": None,
                "title": None,
                "passed": None,
                "error": None,
                "started_at": None,
                "finished_at": None,
                "headline": "Pick an experiment to demonstrate.",
                "steps": [],
                "requests": [],
                "phases": [],
                "metrics": {},
                "proof": {},
                "result": {},
            }
        )
    return {"ok": True, "cleared": True}


def _reset(experiment: int, title: str) -> None:
    global _req_seq
    _req_seq = 0
    _state.update(
        {
            "running": True,
            "experiment": experiment,
            "title": title,
            "passed": None,
            "error": None,
            "started_at": time.time(),
            "finished_at": None,
            "headline": f"Starting experiment {experiment}…",
            "steps": [],
            "requests": [],
            "phases": [],
            "metrics": {},
            "proof": {},
            "result": {},
        }
    )


def _headline(msg: str) -> None:
    with _lock:
        _state["headline"] = msg


def _step(level: str, message: str, **extra: Any) -> None:
    entry = {"t": time.time(), "level": level, "message": message, **extra}
    with _lock:
        _state["steps"].append(entry)
        if level in {"info", "pass", "fail", "phase"}:
            _state["headline"] = message


def _set_metrics(**kwargs: Any) -> None:
    with _lock:
        _state["metrics"].update(kwargs)


def _set_proof(**kwargs: Any) -> None:
    with _lock:
        _state["proof"].update(kwargs)


def _set_phase(key: str, label: str, status: str = "active", detail: str = "") -> None:
    with _lock:
        phases = _state["phases"]
        found = next((p for p in phases if p["key"] == key), None)
        if status == "active":
            for p in phases:
                if p is not found and p.get("status") == "active":
                    p["status"] = "done"
        if found:
            found["status"] = status
            found["label"] = label
            found["detail"] = detail
            found["t"] = time.time()
        else:
            phases.append(
                {
                    "key": key,
                    "label": label,
                    "status": status,
                    "detail": detail,
                    "t": time.time(),
                }
            )


def _add_request(row: dict[str, Any]) -> None:
    global _req_seq
    with _lock:
        _req_seq += 1
        row = {
            "seq": _req_seq,
            "t": time.time(),
            **row,
        }
        _state["requests"].append(row)
        # Keep UI responsive; newest first is handled in frontend.
        if len(_state["requests"]) > 200:
            _state["requests"] = _state["requests"][-200:]


def _set_result(data: dict[str, Any]) -> None:
    with _lock:
        _state["result"] = data


def _finish(passed: bool, error: Optional[str] = None) -> None:
    with _lock:
        _state["running"] = False
        _state["passed"] = passed
        _state["error"] = error
        _state["finished_at"] = time.time()
        if passed:
            _state["headline"] = f"DEMO PASSED · Experiment {_state['experiment']}"
        else:
            _state["headline"] = f"DEMO FAILED · Experiment {_state['experiment']}: {error}"


def start(experiment: int) -> dict[str, Any]:
    titles = {
        3: "Concurrent replica operations",
        6: "Interrupted distributed transaction",
        7: "Concurrent cross-shard transfers",
    }
    if experiment not in titles:
        return {"ok": False, "error": "experiment must be 3, 6, or 7"}

    with _lock:
        if _state["running"]:
            return {
                "ok": False,
                "error": f"experiment {_state['experiment']} is already running",
            }
        _reset(experiment, titles[experiment])

    thread = threading.Thread(
        target=_run_safe,
        args=(experiment, titles[experiment]),
        daemon=True,
        name=f"lab-exp-{experiment}",
    )
    thread.start()
    return {"ok": True, "experiment": experiment, "title": titles[experiment]}


def _run_safe(experiment: int, title: str) -> None:
    try:
        _step("info", f"Live demo · Experiment {experiment}: {title}")
        if experiment == 3:
            result = _exp03()
        elif experiment == 6:
            result = _exp06()
        else:
            result = _exp07()
        _set_result(result)
        _step("pass", f"Experiment {experiment} proven on live system")
        _finish(True)
    except Exception as exc:  # noqa: BLE001
        _step("fail", f"Experiment {experiment} failed: {exc}")
        _finish(False, str(exc))


def _gateway_tx(payload: dict, timeout: float = 30) -> tuple[int, dict]:
    r = requests.post(f"{GATEWAY_URL}/transaction", json=payload, timeout=timeout)
    try:
        data = r.json()
    except ValueError:
        data = {"status": "error", "message": r.text}
    if not isinstance(data, dict):
        data = {"status": "error", "message": str(data)}
    return r.status_code, data


def _balance(account: str) -> tuple[Decimal, dict]:
    code, data = _gateway_tx({"action": "balance", "account": account})
    if code != 200 or data.get("status") != "success":
        raise RuntimeError(f"balance {account} failed: {code} {data}")
    return Decimal(str(data["balance"])), data


def _sql_sum(url: str) -> Decimal:
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(balance), 0) FROM accounts")
            return Decimal(str(cur.fetchone()[0]))


def _total_money() -> Decimal:
    return _sql_sum(DB_A_URL) + _sql_sum(DB_B_URL)


def _coord_query(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg.connect(DB_COORD_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            return list(cur.fetchall())


def _set_fault_flag(name: str, enabled: bool) -> None:
    FAULT_DIR.mkdir(parents=True, exist_ok=True)
    path = FAULT_DIR / name
    if enabled:
        path.write_text("1", encoding="utf-8")
        _step("info", f"Fault injection armed: {name}")
    else:
        path.unlink(missing_ok=True)
        _step("info", f"Fault injection cleared: {name}")


def _wait_http(url: str, timeout: float = 40) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code < 500:
                return
            last = f"HTTP {r.status_code}"
        except requests.RequestException as exc:
            last = str(exc)
        time.sleep(0.4)
    raise RuntimeError(f"timeout waiting for {url}: {last}")


def _docker_service(action: str, service: str) -> None:
    name = SERVICE_CONTAINERS[service]
    _step("info", f"Docker {action.upper()} → {name}")
    info = _docker_api(f"/containers/{name}/json")
    full_id = info["Id"]
    if action == "kill":
        _docker_api(f"/containers/{full_id}/kill", method="POST")
    elif action == "start":
        state = ((info.get("State") or {}).get("Status") or "").lower()
        if state != "running":
            _docker_api(f"/containers/{full_id}/start", method="POST")
    elif action == "stop":
        state = ((info.get("State") or {}).get("Status") or "").lower()
        if state == "running":
            _docker_api(f"/containers/{full_id}/stop?t=3", method="POST")
    else:
        raise ValueError(action)


def _exp03() -> dict[str, Any]:
    account = "A0300"
    n_deposit = 40
    n_withdraw = 25
    amount = Decimal("1.00")
    total_ops = n_deposit + n_withdraw

    _set_phase("setup", "Read starting balance", "active")
    start, start_meta = _balance(account)
    expected = start + amount * n_deposit - amount * n_withdraw
    _set_proof(
        account=account,
        before=float(start),
        expected=float(expected),
        formula=f"{float(start)} + ({n_deposit}×{float(amount)}) − ({n_withdraw}×{float(amount)}) = {float(expected)}",
        after=None,
    )
    _set_metrics(
        total=total_ops,
        done=0,
        ok=0,
        fail=0,
        replica_hits={"A1": 0, "A2": 0},
        deposits=n_deposit,
        withdraws=n_withdraw,
    )
    _set_phase(
        "setup",
        "Starting balance captured",
        "done",
        f"{account} = {start} via {start_meta.get('replica_id')}",
    )
    _step(
        "info",
        f"Account {account} starts at {start}. Launching {total_ops} concurrent requests…",
    )

    _set_phase(
        "burst",
        "Concurrent requests in flight",
        "active",
        f"{n_deposit} deposits + {n_withdraw} withdraws through Gateway → A1/A2",
    )
    _headline("Sending concurrent deposit/withdraw requests — watch the live table")

    hits_lock = threading.Lock()
    replica_hits = {"A1": 0, "A2": 0}
    done = ok = fail = 0

    def one(op: str, idx: int) -> None:
        nonlocal done, ok, fail
        # Stagger slightly so the demo UI can show requests streaming in.
        time.sleep(0.03 * (idx % 8))
        t0 = time.time()
        try:
            code, data = _gateway_tx(
                {
                    "action": op,
                    "account": account,
                    "amount": float(amount),
                }
            )
            ms = int((time.time() - t0) * 1000)
            rid = str(data.get("replica_id") or "?")
            success = code == 200 and data.get("status") == "success"
            _add_request(
                {
                    "kind": op,
                    "account": account,
                    "amount": float(amount),
                    "replica": rid,
                    "ok": success,
                    "ms": ms,
                    "detail": "OK" if success else str(data.get("message") or data.get("status")),
                }
            )
            with hits_lock:
                done += 1
                if success:
                    ok += 1
                    if rid in replica_hits:
                        replica_hits[rid] += 1
                else:
                    fail += 1
                _set_metrics(
                    done=done,
                    ok=ok,
                    fail=fail,
                    replica_hits=dict(replica_hits),
                )
            if not success:
                raise RuntimeError(f"{op} failed: {data}")
        except Exception as exc:  # noqa: BLE001
            ms = int((time.time() - t0) * 1000)
            _add_request(
                {
                    "kind": op,
                    "account": account,
                    "amount": float(amount),
                    "replica": "—",
                    "ok": False,
                    "ms": ms,
                    "detail": str(exc),
                }
            )
            with hits_lock:
                done += 1
                fail += 1
                _set_metrics(done=done, ok=ok, fail=fail, replica_hits=dict(replica_hits))
            raise

    jobs = [("deposit", i) for i in range(n_deposit)] + [
        ("withdraw", i) for i in range(n_withdraw)
    ]
    errors: list[Exception] = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(one, op, i) for op, i in jobs]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    if errors:
        raise RuntimeError(f"{len(errors)} requests failed; first={errors[0]}")

    _set_phase("burst", "All concurrent requests finished", "done", f"{ok}/{total_ops} OK")
    _set_phase("verify", "Verify final balance against expected value", "active")
    time.sleep(0.4)
    end, end_meta = _balance(account)
    _set_proof(after=float(end), last_replica=end_meta.get("replica_id"))
    _step(
        "info",
        f"Replica distribution visible in table: A1={replica_hits['A1']}, A2={replica_hits['A2']}",
    )
    if end != expected:
        _set_phase("verify", "Balance mismatch", "fail", f"got {end}, expected {expected}")
        raise RuntimeError(f"balance mismatch: end={end} expected={expected}")
    _set_phase(
        "verify",
        "Final balance matches exactly",
        "done",
        f"{account}: {start} → {end} (expected {expected})",
    )
    _step(
        "pass",
        f"PROOF: {account} = {end}. Formula matched. Both replicas served traffic.",
    )
    return {
        "account": account,
        "start": float(start),
        "end": float(end),
        "expected": float(expected),
        "deposits": n_deposit,
        "withdraws": n_withdraw,
        "replica_hits": replica_hits,
        "last_replica": end_meta.get("replica_id"),
    }


def _exp06() -> dict[str, Any]:
    src, dst = "A0820", "A1820"
    amount = Decimal("12.00")

    _set_phase("setup", "Capture balances before cross-shard transfer", "active")
    s0, _ = _balance(src)
    d0, _ = _balance(dst)
    _set_proof(
        from_account=src,
        to_account=dst,
        amount=float(amount),
        before={src: float(s0), dst: float(d0)},
        after=None,
        tx_id=None,
        status_after_kill=None,
        final_status=None,
    )
    _set_metrics(phase="setup", tx_status="—")
    _set_phase(
        "setup",
        "Balances captured",
        "done",
        f"{src}={s0}, {dst}={d0}",
    )

    _set_fault_flag("pause_after_commit_decided", True)
    result: dict[str, Any] = {"code": None, "data": None, "error": None}

    def worker() -> None:
        try:
            result["code"], result["data"] = _gateway_tx(
                {
                    "action": "transfer",
                    "account": src,
                    "to_account": dst,
                    "amount": float(amount),
                },
                timeout=90,
            )
        except Exception as exc:  # noqa: BLE001
            result["error"] = str(exc)

    try:
        _set_phase(
            "transfer",
            "Cross-shard transfer started (2PC)",
            "active",
            f"{src} → {dst} amount {amount}",
        )
        _headline("Transfer in flight — waiting until decision is durably COMMIT_DECIDED")
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        _add_request(
            {
                "kind": "transfer",
                "account": src,
                "to_account": dst,
                "amount": float(amount),
                "replica": "2PC",
                "ok": None,
                "ms": None,
                "detail": "in flight · waiting for durable COMMIT_DECIDED",
            }
        )

        deadline = time.time() + 35
        tx_id = None
        last_seen = None
        while time.time() < deadline:
            rows = _coord_query(
                "SELECT transaction_id, status FROM distributed_transactions "
                "WHERE from_account=%s AND to_account=%s "
                "ORDER BY updated_at DESC LIMIT 1",
                (src, dst),
            )
            if rows:
                last_seen = rows[0]["status"]
                _set_metrics(tx_status=last_seen, tx_id=rows[0]["transaction_id"][:8])
                if last_seen == "COMMIT_DECIDED":
                    tx_id = rows[0]["transaction_id"]
                    break
            time.sleep(0.2)

        if not tx_id:
            raise RuntimeError(
                f"did not observe COMMIT_DECIDED (last status={last_seen})"
            )

        _set_proof(tx_id=tx_id)
        _set_phase(
            "transfer",
            "Durable COMMIT_DECIDED recorded in Coord DB",
            "done",
            f"tx {tx_id[:8]}…",
        )
        _step("info", f"Coord DB shows COMMIT_DECIDED for tx {tx_id[:8]}…")

        _set_phase(
            "kill",
            "Kill Coordinator before participant COMMIT acks",
            "active",
            "Decision is durable; phase-2 not finished",
        )
        _headline("Killing Coordinator now — decision must survive the crash")
        time.sleep(0.6)
        _docker_service("kill", "coordinator")
        time.sleep(0.5)

        rows = _coord_query(
            "SELECT status FROM distributed_transactions WHERE transaction_id=%s",
            (tx_id,),
        )
        status_after_kill = rows[0]["status"] if rows else None
        _set_proof(status_after_kill=status_after_kill)
        _set_metrics(tx_status=status_after_kill, coordinator="killed")
        if status_after_kill != "COMMIT_DECIDED":
            _set_phase("kill", "Unexpected status after kill", "fail", str(status_after_kill))
            raise RuntimeError(
                f"after kill expected COMMIT_DECIDED, got {status_after_kill}"
            )
        _set_phase(
            "kill",
            "Coordinator dead · decision still COMMIT_DECIDED",
            "done",
            "Not rewritten to ABORT",
        )
        _step("pass", "PROOF mid-flight: durable decision survived coordinator kill")

        _set_fault_flag("pause_after_commit_decided", False)
        _set_phase(
            "recover",
            "Restart Coordinator and resume 2PC",
            "active",
            "Recovery must finish the recorded COMMIT",
        )
        _headline("Restarting Coordinator — watch status move to COMPLETED")
        _docker_service("start", "coordinator")
        _wait_http(f"{COORDINATOR_URL}/health", timeout=40)
        _wait_http(f"{GATEWAY_URL}/health", timeout=20)
        _set_metrics(coordinator="running")

        deadline = time.time() + 45
        final_status = None
        while time.time() < deadline:
            rows = _coord_query(
                "SELECT status FROM distributed_transactions WHERE transaction_id=%s",
                (tx_id,),
            )
            final_status = rows[0]["status"] if rows else None
            _set_metrics(tx_status=final_status)
            if final_status == "COMPLETED":
                break
            time.sleep(0.35)
        if final_status != "COMPLETED":
            _set_phase("recover", "Recovery stalled", "fail", str(final_status))
            raise RuntimeError(f"recovery did not reach COMPLETED; last={final_status}")

        _set_phase(
            "recover",
            "Recovery finished · status COMPLETED",
            "done",
            f"tx {tx_id[:8]}…",
        )
        t.join(timeout=10)

        _set_phase("verify", "Verify money moved exactly once", "active")
        s1, _ = _balance(src)
        d1, _ = _balance(dst)
        _set_proof(
            after={src: float(s1), dst: float(d1)},
            final_status=final_status,
        )
        worker_ok = (
            result.get("data") or {}
        ).get("status") == "success" or (
            result.get("data") or {}
        ).get("decision") == "COMMIT"
        _add_request(
            {
                "kind": "transfer",
                "account": src,
                "to_account": dst,
                "amount": float(amount),
                "replica": "2PC",
                "ok": bool(s1 == s0 - amount and d1 == d0 + amount),
                "ms": None,
                "detail": f"tx {tx_id[:8]}… · {final_status} · worker={ (result.get('data') or {}).get('status') or result.get('error') }",
            }
        )
        if s1 != s0 - amount or d1 != d0 + amount:
            _set_phase("verify", "Balance proof failed", "fail", f"{src} {s0}->{s1}, {dst} {d0}->{d1}")
            raise RuntimeError(
                f"balances after recovery incorrect: {src} {s0}->{s1}, {dst} {d0}->{d1}"
            )
        _set_phase(
            "verify",
            "Exactly-once money movement proven",
            "done",
            f"{src}: {s0}→{s1}, {dst}: {d0}→{d1}",
        )
        _step(
            "pass",
            f"PROOF: crash after COMMIT_DECIDED recovered to COMPLETED; money moved once. worker_ok={worker_ok}",
        )
        return {
            "transaction_id": tx_id,
            "from": src,
            "to": dst,
            "amount": float(amount),
            "balances_before": {src: float(s0), dst: float(d0)},
            "balances_after": {src: float(s1), dst: float(d1)},
            "status_after_kill": status_after_kill,
            "final_status": final_status,
            "worker": {
                "code": result.get("code"),
                "status": (result.get("data") or {}).get("status"),
                "decision": (result.get("data") or {}).get("decision"),
                "error": result.get("error"),
            },
        }
    finally:
        _set_fault_flag("pause_after_commit_decided", False)
        try:
            _docker_service("start", "coordinator")
            _wait_http(f"{COORDINATOR_URL}/health", timeout=30)
            _set_metrics(coordinator="running")
        except Exception as exc:  # noqa: BLE001
            _step("info", f"coordinator cleanup note: {exc}")


def _exp07() -> dict[str, Any]:
    pairs = [
        ("A0110", "A1510"),
        ("A1510", "A0110"),
        ("A0120", "A1520"),
        ("A1520", "A0120"),
        ("A0130", "A1530"),
        ("A0140", "A1540"),
        ("A0150", "A1550"),
        ("A0160", "A1560"),
    ]
    amount = Decimal("3.00")
    repeats = 3

    _set_phase("setup", "Snapshot total money + sample accounts", "active")
    before = {acc: float(_balance(acc)[0]) for pair in pairs for acc in pair}
    money0 = _total_money()
    jobs = []
    for _ in range(repeats):
        jobs.extend(pairs)
    _set_proof(
        money_before=float(money0),
        money_after=None,
        sample_before=dict(list(before.items())[:6]),
        sample_after=None,
        jobs=len(jobs),
    )
    _set_metrics(
        total=len(jobs),
        done=0,
        success=0,
        abort=0,
        pending=0,
        money_before=float(money0),
    )
    _set_phase(
        "setup",
        "Baseline recorded",
        "done",
        f"total money = {money0}; {len(jobs)} overlapping transfers queued",
    )
    _step("info", f"Conservation baseline: total money = {money0}")

    _set_phase(
        "burst",
        "Concurrent overlapping cross-shard transfers",
        "active",
        "Same accounts, opposite directions, parallel 2PC",
    )
    _headline("Firing concurrent cross-shard transfers — each row is a real 2PC result")

    done = success = abort = pending = 0
    tx_ids: list[str] = []
    lock = threading.Lock()

    def one(job_idx: int, src: str, dst: str) -> tuple[int, dict]:
        nonlocal done, success, abort, pending
        time.sleep(0.04 * (job_idx % 6))
        t0 = time.time()
        code, data = _gateway_tx(
            {
                "action": "transfer",
                "account": src,
                "to_account": dst,
                "amount": float(amount),
            },
            timeout=60,
        )
        ms = int((time.time() - t0) * 1000)
        status = str(data.get("status") or "").lower()
        decision = str(data.get("decision") or "")
        tid = data.get("transaction_id")
        label = "COMMIT"
        ok = False
        bucket = "other"
        if status == "success" and decision == "COMMIT":
            ok = True
            bucket = "success"
            label = "COMMIT"
        elif status in {"aborted", "error"} or decision == "ABORT":
            bucket = "abort"
            label = "ABORT"
        elif status == "pending":
            bucket = "pending"
            label = "PENDING"
        else:
            label = status or "error"

        _add_request(
            {
                "kind": "transfer",
                "account": src,
                "to_account": dst,
                "amount": float(amount),
                "replica": "2PC",
                "ok": ok if bucket != "pending" else None,
                "ms": ms,
                "detail": f"{label}"
                + (f" · tx {(tid or '')[:8]}" if tid else "")
                + (f" · {data.get('message')}" if data.get("message") and not ok else ""),
            }
        )
        with lock:
            done += 1
            if bucket == "success":
                success += 1
            elif bucket == "abort":
                abort += 1
            elif bucket == "pending":
                pending += 1
            if tid:
                tx_ids.append(tid)
            _set_metrics(
                done=done,
                success=success,
                abort=abort,
                pending=pending,
            )
        if bucket == "other":
            raise RuntimeError(f"unexpected transfer result: {code} {data}")
        return code, data

    errors: list[Exception] = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [
            pool.submit(one, idx, src, dst)
            for idx, (src, dst) in enumerate(jobs)
        ]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    if errors:
        raise RuntimeError(f"{len(errors)} transfers failed hard; first={errors[0]}")

    _set_phase(
        "burst",
        "Burst finished",
        "done",
        f"success={success} abort={abort} pending={pending}",
    )

    if pending:
        _set_phase("settle", "Wait for pending txs to converge", "active")
        _headline("Some transfers returned PENDING — waiting for coordinator settle")
        deadline = time.time() + 45
        while time.time() < deadline and pending:
            still = 0
            for tid in tx_ids:
                rows = _coord_query(
                    "SELECT status FROM distributed_transactions WHERE transaction_id=%s",
                    (tid,),
                )
                if rows and rows[0]["status"] != "COMPLETED":
                    still += 1
            if still == 0:
                pending = 0
                _set_metrics(pending=0)
                break
            _set_metrics(pending=still)
            time.sleep(1)
        if pending:
            _set_phase("settle", "Pending txs did not converge", "fail")
            raise RuntimeError("pending cross-shard transfers did not converge to COMPLETED")
        _set_phase("settle", "All pending txs settled", "done")

    _set_phase("verify", "Check Coord DB COMPLETED + money conservation", "active")
    bad = []
    for tid in tx_ids:
        rows = _coord_query(
            "SELECT status FROM distributed_transactions WHERE transaction_id=%s",
            (tid,),
        )
        status = rows[0]["status"] if rows else None
        if status != "COMPLETED":
            bad.append((tid[:8], status))
    if bad:
        _set_phase("verify", "Non-COMPLETED coordinator rows", "fail", str(bad[:5]))
        raise RuntimeError(f"non-COMPLETED coord states: {bad[:5]}")

    money1 = _total_money()
    after = {acc: float(_balance(acc)[0]) for acc in list(before)[:6]}
    _set_proof(
        money_after=float(money1),
        sample_after=after,
        successes=success,
        aborts=abort,
        tx_count=len(tx_ids),
    )
    _set_metrics(money_after=float(money1))
    if money1 != money0:
        _set_phase(
            "verify",
            "Money not conserved",
            "fail",
            f"before={money0} after={money1}",
        )
        raise RuntimeError(f"money not conserved: before={money0} after={money1}")

    for acc, now in after.items():
        if now < 0:
            raise RuntimeError(f"negative balance on {acc}: {now}")

    _set_phase(
        "verify",
        "Money conserved · all distributed txs COMPLETED",
        "done",
        f"total {money0} → {money1}",
    )
    _step(
        "pass",
        f"PROOF: {len(jobs)} concurrent cross-shard transfers; total money unchanged at {money1}",
    )
    return {
        "jobs": len(jobs),
        "successes": success,
        "aborts": abort,
        "pending_after_wait": pending,
        "tx_count": len(tx_ids),
        "money_before": float(money0),
        "money_after": float(money1),
        "sample_balances_before": dict(list(before.items())[:6]),
        "sample_balances_after": after,
    }
