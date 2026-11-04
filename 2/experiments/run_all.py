"""Run Assignment 2 required experiments 1–7."""

from __future__ import annotations

import sys

from exp01_shard_distribution import experiment as e1
from exp02_replica_load import experiment as e2
from exp03_concurrent_replicas import experiment as e3
from exp04_replica_interruption import experiment as e4
from exp05_transfers import experiment as e5
from exp06_interrupted_2pc import experiment as e6
from exp07_concurrent_cross_shard import experiment as e7
from common import ensure_stack, log, run_experiment


EXPERIMENTS = [
    (1, "Shard distribution", e1),
    (2, "Replica load distribution", e2),
    (3, "Concurrent replica operations", e3),
    (4, "Replica interruption", e4),
    (5, "Local and cross-shard transfers", e5),
    (6, "Interrupted distributed transaction", e6),
    (7, "Concurrent cross-shard transfers", e7),
]


def main() -> int:
    only = None
    if len(sys.argv) > 1:
        only = {int(x) for x in sys.argv[1:]}

    ensure_stack()
    results = []
    for num, title, fn in EXPERIMENTS:
        if only and num not in only:
            continue
        results.append((num, title, run_experiment(num, title, fn)))

    log("")
    log("=" * 72)
    log("SUMMARY")
    log("=" * 72)
    failed = 0
    for num, title, passed in results:
        log(f"  [{'PASS' if passed else 'FAIL'}]  {num}. {title}")
        if not passed:
            failed += 1
    log("")
    if failed:
        log(f"{failed} experiment(s) failed")
        return 1
    log("All selected experiments passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
