"""Experiment 2: Replica load distribution (Round Robin)."""

from __future__ import annotations

from collections import Counter

from common import balance, fail, info, ok, parallel_map, run_experiment


def experiment() -> None:
    account = "A0100"
    n = 40

    def one(_: int) -> str:
        _, data = balance(account)
        rid = str(data.get("replica_id") or "?")
        return rid

    replicas = parallel_map(one, list(range(n)), workers=8)
    counts = Counter(replicas)
    info(f"replica hits over {n} requests: {dict(counts)}")

    if len(counts) < 2:
        fail(f"expected multiple replicas for shard A, got {counts}")
    if counts.get("A1", 0) == 0 or counts.get("A2", 0) == 0:
        fail(f"both A1 and A2 must handle traffic, got {counts}")

    # Roughly balanced under RR (allow skew from concurrency).
    ratio = min(counts["A1"], counts["A2"]) / max(counts["A1"], counts["A2"])
    if ratio < 0.25:
        fail(f"load too skewed for Round Robin: {counts}")
    ok(f"load spread across A1/A2 (ratio={ratio:.2f})")


def main() -> None:
    raise SystemExit(0 if run_experiment(2, "Replica load distribution", experiment) else 1)


if __name__ == "__main__":
    main()
