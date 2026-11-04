"""Experiment 6: Interrupt coordinator after durable COMMIT, then recover."""

from __future__ import annotations

import threading
import time
from decimal import Decimal

from common import (
    COORD,
    GATEWAY,
    balance,
    docker_kill,
    docker_start,
    fail,
    info,
    ok,
    run_experiment,
    set_fault_flag,
    sql,
    transfer,
    wait_coord_status,
    wait_http,
)


def experiment() -> None:
    # Fresh accounts so reruns are not confused by a prior successful recovery.
    src, dst = "A0820", "A1820"
    amount = Decimal("12.00")
    s0, _ = balance(src)
    d0, _ = balance(dst)

    # If a previous interrupted attempt already moved money, use a fresh pair amount check
    # against current balances after recovery verification.
    set_fault_flag("pause_after_commit_decided", True)
    result: dict = {"code": None, "data": None, "error": None}

    def worker() -> None:
        try:
            result["code"], result["data"] = transfer(src, dst, amount, timeout=90)
        except Exception as exc:  # noqa: BLE001
            result["error"] = exc

    try:
        t = threading.Thread(target=worker, daemon=True)
        t.start()

        deadline = time.time() + 35
        tx_id = None
        while time.time() < deadline:
            row = sql(
                "coord",
                "SELECT transaction_id, status FROM distributed_transactions "
                f"WHERE from_account='{src}' AND to_account='{dst}' "
                "AND status='COMMIT_DECIDED' "
                "ORDER BY updated_at DESC LIMIT 1;",
            ).strip()
            if row:
                tx_id = row.split("|", 1)[0]
                break
            time.sleep(0.15)

        if not tx_id:
            fail("did not observe COMMIT_DECIDED while fault pause was active")

        info(f"observed durable COMMIT_DECIDED for tx={tx_id[:8]} - killing coordinator now")
        # Kill immediately while pause flag still holds Phase-2 from starting.
        docker_kill("coordinator")

        row = wait_coord_status(tx_id, {"COMMIT_DECIDED"}, timeout=5)
        ok(f"after coordinator kill, durable status remains {row['status']}")

        set_fault_flag("pause_after_commit_decided", False)
        info("restarting coordinator for recovery")
        docker_start("coordinator")
        wait_http(f"{COORD}/health", timeout=40)
        wait_http(f"{GATEWAY}/health", timeout=20)

        final = wait_coord_status(tx_id, {"COMPLETED"}, timeout=45)
        ok(f"recovery completed tx={tx_id[:8]} status={final['status']}")

        t.join(timeout=10)
        s1, _ = balance(src)
        d1, _ = balance(dst)
        if s1 != s0 - amount or d1 != d0 + amount:
            fail(
                f"balances after recovery incorrect: {src} {s0}->{s1}, {dst} {d0}->{d1}; "
                f"worker={result}"
            )
        ok(f"money moved exactly once after recovery: {src}-={amount}, {dst}+={amount}")
    finally:
        set_fault_flag("pause_after_commit_decided", False)
        # Always try to bring coordinator back for later experiments.
        try:
            docker_start("coordinator")
            wait_http(f"{COORD}/health", timeout=30)
        except Exception:  # noqa: BLE001
            pass


def main() -> None:
    raise SystemExit(
        0 if run_experiment(6, "Interrupted distributed transaction", experiment) else 1
    )


if __name__ == "__main__":
    main()
