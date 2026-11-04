"""Experiment 4: Stop one replica under load; gateway fails over."""

from __future__ import annotations

import time
from collections import Counter

from common import (
    balance,
    docker_start,
    docker_stop,
    fail,
    info,
    ok,
    parallel_map,
    run_experiment,
    wait_http,
    wait_replica_health,
)
from common import GATEWAY


def experiment() -> None:
    account = "A0400"
    try:
        docker_stop("bank-a2")
        wait_replica_health("bank-a2", healthy=False, timeout=20)
        info("gateway sees bank-a2 as unhealthy")

        def one(_: int) -> str:
            _, data = balance(account)
            return str(data.get("replica_id"))

        hits = parallel_map(one, list(range(30)), workers=10)
        counts = Counter(hits)
        info(f"hits while A2 down: {dict(counts)}")
        if counts.get("A2", 0) != 0:
            fail(f"gateway still routed to A2: {counts}")
        if counts.get("A1", 0) == 0:
            fail(f"expected traffic on A1, got {counts}")
        ok("all requests served by healthy replica A1")
    finally:
        docker_start("bank-a2")
        wait_http(f"{GATEWAY}/health")
        # Give health loop a moment; A2 may need a few seconds after start.
        time.sleep(3)
        wait_replica_health("bank-a2", healthy=True, timeout=30)
        ok("bank-a2 restored and healthy again")


def main() -> None:
    raise SystemExit(0 if run_experiment(4, "Replica interruption", experiment) else 1)


if __name__ == "__main__":
    main()
