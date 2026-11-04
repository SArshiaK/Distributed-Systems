"""Shared helpers for Assignment 2 required experiments."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

import requests

ROOT = Path(__file__).resolve().parent
FAULTS = ROOT / "faults"
GATEWAY = "http://127.0.0.1:8080"
COORD = "http://127.0.0.1:8090"
PROJECT = "2"

DB_CONTAINERS = {"A": "2-db-a-1", "B": "2-db-b-1", "coord": "2-db-coord-1"}
SERVICE_CONTAINERS = {
    "bank-a1": "2-bank-a1-1",
    "bank-a2": "2-bank-a2-1",
    "bank-b1": "2-bank-b1-1",
    "bank-b2": "2-bank-b2-1",
    "gateway": "2-gateway-1",
    "coordinator": "2-coordinator-1",
}


class ExperimentError(RuntimeError):
    pass


def log(msg: str) -> None:
    # Windows consoles may not support Unicode arrows; keep ASCII-safe output.
    text = (
        str(msg)
        .replace("\u2192", "->")
        .replace("\u2026", "...")
        .replace("\u00b7", "*")
    )
    try:
        print(text, flush=True)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"), flush=True)


def ok(msg: str) -> None:
    log(f"  PASS  {msg}")


def info(msg: str) -> None:
    log(f"  ·  {msg}")


def fail(msg: str) -> None:
    raise ExperimentError(msg)


def run(cmd: list[str], check: bool = True, timeout: float = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(ROOT.parent),
    )


def docker(*args: str, check: bool = True, timeout: float = 90) -> str:
    cp = run(["docker", *args], check=check, timeout=timeout)
    return (cp.stdout or "").strip()


def compose(*args: str, check: bool = True) -> str:
    return docker("compose", *args, check=check)


def wait_http(url: str, timeout: float = 45) -> None:
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
        time.sleep(0.5)
    fail(f"timeout waiting for {url}: {last}")


def ensure_stack() -> None:
    # Clear leftover fault flags from previous runs.
    FAULTS.mkdir(parents=True, exist_ok=True)
    for p in FAULTS.glob("*"):
        if p.name != ".gitkeep":
            p.unlink(missing_ok=True)

    # Heal services left stopped by prior fault experiments.
    for service in ("bank-a1", "bank-a2", "bank-b1", "bank-b2", "gateway", "coordinator"):
        name = SERVICE_CONTAINERS[service]
        state = docker("inspect", "-f", "{{.State.Running}}", name, check=False)
        if state.strip().lower() != "true":
            info(f"restarting stopped container {name}")
            docker("start", name, check=False)

    wait_http(f"{GATEWAY}/health")
    wait_http(f"{COORD}/health")
    # Give gateway health loop time to mark replicas.
    time.sleep(2.5)


def gateway_tx(payload: dict, timeout: float = 30) -> tuple[int, dict]:
    r = requests.post(f"{GATEWAY}/transaction", json=payload, timeout=timeout)
    try:
        data = r.json()
    except ValueError:
        data = {"status": "error", "message": r.text}
    return r.status_code, data


def balance(account: str) -> tuple[Decimal, dict]:
    code, data = gateway_tx({"action": "balance", "account": account})
    if code != 200 or data.get("status") != "success":
        fail(f"balance {account} failed: {code} {data}")
    return Decimal(str(data["balance"])), data


def deposit(account: str, amount: float | str | Decimal) -> dict:
    code, data = gateway_tx(
        {"action": "deposit", "account": account, "amount": float(amount)}
    )
    if code != 200 or data.get("status") != "success":
        fail(f"deposit {account} failed: {code} {data}")
    return data


def withdraw(account: str, amount: float | str | Decimal) -> dict:
    code, data = gateway_tx(
        {"action": "withdraw", "account": account, "amount": float(amount)}
    )
    if code != 200 or data.get("status") != "success":
        fail(f"withdraw {account} failed: {code} {data}")
    return data


def transfer(src: str, dst: str, amount: float | str | Decimal, timeout: float = 45) -> tuple[int, dict]:
    return gateway_tx(
        {
            "action": "transfer",
            "account": src,
            "to_account": dst,
            "amount": float(amount),
        },
        timeout=timeout,
    )


def shard_map() -> dict:
    r = requests.get(f"{GATEWAY}/shard-map", timeout=5)
    r.raise_for_status()
    return r.json()


def sql(db: str, query: str) -> str:
    container = DB_CONTAINERS[db]
    dbname = "coord" if db == "coord" else "bank"
    out = docker(
        "exec",
        container,
        "psql",
        "-U",
        "bank",
        "-d",
        dbname,
        "-At",
        "-F",
        "|",
        "-c",
        query,
    )
    return out


def account_in_db(db: str, account: str) -> bool:
    row = sql(db, f"SELECT 1 FROM accounts WHERE account_id = '{account}';")
    return row.strip() == "1"


def account_balance_db(db: str, account: str) -> Decimal:
    row = sql(db, f"SELECT balance FROM accounts WHERE account_id = '{account}';")
    if not row.strip():
        fail(f"{account} missing in db-{db}")
    return Decimal(row.strip())


def total_money() -> Decimal:
    a = sql("A", "SELECT COALESCE(SUM(balance),0) FROM accounts;")
    b = sql("B", "SELECT COALESCE(SUM(balance),0) FROM accounts;")
    return Decimal(a.strip()) + Decimal(b.strip())


def coord_tx(tx_id: str) -> Optional[dict]:
    row = sql(
        "coord",
        "SELECT transaction_id, status, from_account, to_account, amount, "
        "from_shard, to_shard "
        f"FROM distributed_transactions WHERE transaction_id = '{tx_id}';",
    )
    if not row.strip():
        return None
    parts = row.strip().split("|")
    return {
        "transaction_id": parts[0],
        "status": parts[1],
        "from_account": parts[2],
        "to_account": parts[3],
        "amount": Decimal(parts[4]),
        "from_shard": parts[5],
        "to_shard": parts[6],
    }


def wait_coord_status(tx_id: str, wanted: set[str], timeout: float = 40) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        row = coord_tx(tx_id)
        last = row
        if row and row["status"] in wanted:
            return row
        time.sleep(0.25)
    fail(f"tx {tx_id} did not reach {wanted}; last={last}")


def latest_coord_tx() -> Optional[dict]:
    row = sql(
        "coord",
        "SELECT transaction_id, status FROM distributed_transactions "
        "ORDER BY updated_at DESC LIMIT 1;",
    )
    if not row.strip():
        return None
    tid, status = row.strip().split("|", 1)
    return {"transaction_id": tid, "status": status}


def docker_stop(service: str) -> None:
    name = SERVICE_CONTAINERS[service]
    info(f"docker stop {name}")
    docker("stop", name)


def docker_kill(service: str) -> None:
    name = SERVICE_CONTAINERS[service]
    info(f"docker kill {name}")
    docker("kill", name)


def docker_start(service: str) -> None:
    name = SERVICE_CONTAINERS[service]
    info(f"docker start {name}")
    docker("start", name)


def wait_replica_health(service: str, healthy: bool, timeout: float = 30) -> None:
    """Wait until gateway shard-map reports replica health."""
    needle = service  # bank-a2
    deadline = time.time() + timeout
    while time.time() < deadline:
        sm = shard_map()
        for meta in (sm.get("shards") or {}).values():
            for rep in meta.get("replicas") or []:
                if needle in (rep.get("url") or ""):
                    if bool(rep.get("healthy")) == healthy:
                        return
        time.sleep(0.5)
    fail(f"replica {service} did not become healthy={healthy}")


def set_fault_flag(name: str, enabled: bool) -> Path:
    FAULTS.mkdir(parents=True, exist_ok=True)
    path = FAULTS / name
    if enabled:
        path.write_text("1", encoding="utf-8")
        info(f"fault flag ON: {name}")
    else:
        path.unlink(missing_ok=True)
        info(f"fault flag OFF: {name}")
    return path


def parallel_map(fn: Callable, items: list, workers: int = 16) -> list:
    results = []
    errors = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fn, item): item for item in items}
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    if errors:
        fail(f"{len(errors)} parallel tasks failed; first={errors[0]}")
    return results


def banner(num: int, title: str) -> None:
    log("")
    log("=" * 72)
    log(f"EXPERIMENT {num}: {title}")
    log("=" * 72)


def run_experiment(num: int, title: str, fn: Callable[[], None]) -> bool:
    banner(num, title)
    try:
        ensure_stack()
        fn()
        log(f"EXPERIMENT {num} PASSED")
        return True
    except Exception as exc:  # noqa: BLE001
        log(f"EXPERIMENT {num} FAILED: {exc}")
        return False
