"""Experiment 5: Same-shard vs cross-shard transfers."""

from __future__ import annotations

from decimal import Decimal

from common import (
    balance,
    coord_tx,
    fail,
    info,
    ok,
    run_experiment,
    transfer,
)


def experiment() -> None:
    # Same-shard: A0600 -> A0700 (both shard A)
    a_src, a_dst = "A0600", "A0700"
    b_src, b_dst = "A0500", "A1500"
    amount = Decimal("15.00")

    s0, _ = balance(a_src)
    d0, _ = balance(a_dst)
    code, data = transfer(a_src, a_dst, amount)
    if code != 200 or data.get("status") != "success":
        fail(f"same-shard transfer failed: {code} {data}")
    if data.get("mode") not in {None, "same_shard"} and data.get("decision"):
        fail(f"same-shard must not go through 2PC decision; got {data}")
    s1, _ = balance(a_src)
    d1, _ = balance(a_dst)
    if s1 != s0 - amount or d1 != d0 + amount:
        fail(f"same-shard balances wrong: {s0}->{s1}, {d0}->{d1}")
    ok(
        f"same-shard {a_src}->{a_dst}: local atomic DB tx on shard "
        f"{data.get('shard_id')} / replica {data.get('replica_id')} (no Coordinator)"
    )
    info("why: both accounts share one authoritative DB -> one local transaction is enough")

    # Cross-shard: A0500 -> A1500
    s0, _ = balance(b_src)
    d0, _ = balance(b_dst)
    code, data = transfer(b_src, b_dst, amount)
    if code != 200 or data.get("status") != "success":
        fail(f"cross-shard transfer failed: {code} {data}")
    if data.get("mode") != "cross_shard" or data.get("decision") != "COMMIT":
        fail(f"expected cross_shard COMMIT, got {data}")
    tx_id = data.get("transaction_id")
    if not tx_id:
        fail("cross-shard response missing transaction_id")
    row = coord_tx(tx_id)
    if not row or row["status"] != "COMPLETED":
        fail(f"coord DB status not COMPLETED: {row}")
    s1, _ = balance(b_src)
    d1, _ = balance(b_dst)
    if s1 != s0 - amount or d1 != d0 + amount:
        fail(f"cross-shard balances wrong: {s0}->{s1}, {d0}->{d1}")
    ok(
        f"cross-shard {b_src}->{b_dst}: 2PC via Coordinator, tx={tx_id[:8]}... "
        f"coord status={row['status']}"
    )
    info(
        "why: accounts live on different DBs -> need prepare/commit so only one side "
        "cannot permanently commit"
    )


def main() -> None:
    raise SystemExit(
        0 if run_experiment(5, "Local and cross-shard transfers", experiment) else 1
    )


if __name__ == "__main__":
    main()
