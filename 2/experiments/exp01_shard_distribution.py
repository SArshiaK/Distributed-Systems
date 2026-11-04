"""Experiment 1: Shard distribution + gateway routing."""

from __future__ import annotations

from common import (
    account_in_db,
    balance,
    fail,
    info,
    ok,
    run_experiment,
    shard_map,
)


def experiment() -> None:
    samples = {
        "A": ["A0001", "A0500", "A1000"],
        "B": ["A1001", "A1500", "A2000"],
    }

    for account in samples["A"]:
        if not account_in_db("A", account):
            fail(f"{account} missing from Database A")
        if account_in_db("B", account):
            fail(f"{account} must NOT exist in Database B")
        ok(f"{account} lives only in Database A")

    for account in samples["B"]:
        if not account_in_db("B", account):
            fail(f"{account} missing from Database B")
        if account_in_db("A", account):
            fail(f"{account} must NOT exist in Database A")
        ok(f"{account} lives only in Database B")

    sm = shard_map()
    info(f"gateway epoch={sm.get('epoch')} shards={list((sm.get('shards') or {}).keys())}")

    _, data_a = balance("A0500")
    if data_a.get("shard_id") != "A":
        fail(f"A0500 routed to shard {data_a.get('shard_id')}, expected A")
    ok(f"Gateway routed A0500 -> shard A / replica {data_a.get('replica_id')}")

    _, data_b = balance("A1500")
    if data_b.get("shard_id") != "B":
        fail(f"A1500 routed to shard {data_b.get('shard_id')}, expected B")
    ok(f"Gateway routed A1500 -> shard B / replica {data_b.get('replica_id')}")


def main() -> None:
    raise SystemExit(0 if run_experiment(1, "Shard distribution", experiment) else 1)


if __name__ == "__main__":
    main()
