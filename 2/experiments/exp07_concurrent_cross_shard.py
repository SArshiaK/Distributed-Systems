"""Experiment 7: Concurrent cross-shard transfers + conservation of money."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

from common import (
    balance,
    fail,
    info,
    ok,
    run_experiment,
    sql,
    total_money,
    transfer,
)


def experiment() -> None:
    # Overlapping accounts across opposite directions.
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

    before = {acc: balance(acc)[0] for pair in pairs for acc in pair}
    money0 = total_money()
    info(f"total money before={money0}")

    jobs = []
    for _ in range(repeats):
        jobs.extend(pairs)

    results = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(transfer, a, b, amount, 60) for a, b in jobs]
        for fut in as_completed(futs):
            results.append(fut.result())

    successes = 0
    aborts = 0
    pending = 0
    tx_ids = []
    for code, data in results:
        status = str(data.get("status") or "").lower()
        if status == "success" and data.get("decision") == "COMMIT":
            successes += 1
            if data.get("transaction_id"):
                tx_ids.append(data["transaction_id"])
        elif status in {"aborted", "error"} or data.get("decision") == "ABORT":
            aborts += 1
            if data.get("transaction_id"):
                tx_ids.append(data["transaction_id"])
        elif status == "pending":
            pending += 1
            if data.get("transaction_id"):
                tx_ids.append(data["transaction_id"])
        else:
            fail(f"unexpected transfer result: {code} {data}")

    info(f"results: success={successes} abort={aborts} pending={pending} total={len(results)}")

    # Any PENDING must finish via coordinator recovery.
    if pending:
        import time

        deadline = time.time() + 45
        while time.time() < deadline and pending:
            still = 0
            for tid in tx_ids:
                row = sql(
                    "coord",
                    "SELECT status FROM distributed_transactions "
                    f"WHERE transaction_id='{tid}';",
                ).strip()
                if row and row not in {"COMPLETED"}:
                    still += 1
            if still == 0:
                pending = 0
                break
            time.sleep(1)
        if pending:
            fail("pending cross-shard transfers did not converge to COMPLETED")

    # Coord rows for known txs should be COMPLETED (commit or abort path finishes).
    bad = []
    for tid in tx_ids:
        row = sql(
            "coord",
            "SELECT status FROM distributed_transactions "
            f"WHERE transaction_id='{tid}';",
        ).strip()
        if row != "COMPLETED":
            bad.append((tid[:8], row))
    if bad:
        fail(f"non-COMPLETED coord states: {bad[:5]}")
    ok(f"all {len(tx_ids)} recorded distributed txs are COMPLETED")

    money1 = total_money()
    if money1 != money0:
        fail(f"money not conserved: before={money0} after={money1}")
    ok(f"conservation of money holds: total={money1}")

    # Spot-check a few accounts still readable and non-negative.
    for acc, old in list(before.items())[:4]:
        now, _ = balance(acc)
        if now < 0:
            fail(f"negative balance on {acc}: {now}")
        info(f"{acc}: {old} -> {now}")
    ok("concurrent overlapping cross-shard transfers finished safely")


def main() -> None:
    raise SystemExit(
        0 if run_experiment(7, "Concurrent cross-shard transfers", experiment) else 1
    )


if __name__ == "__main__":
    main()
