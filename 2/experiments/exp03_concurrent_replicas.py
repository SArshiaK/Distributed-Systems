"""Experiment 3: Concurrent ops across replicas keep correct balances."""

from __future__ import annotations

from decimal import Decimal

from common import balance, deposit, fail, ok, parallel_map, run_experiment, withdraw


def experiment() -> None:
    account = "A0300"
    start, _ = balance(account)
    n_deposit = 40
    n_withdraw = 25
    amount = Decimal("1.00")

    def do_deposit(_: int) -> None:
        deposit(account, amount)

    def do_withdraw(_: int) -> None:
        withdraw(account, amount)

    parallel_map(do_deposit, list(range(n_deposit)), workers=20)
    parallel_map(do_withdraw, list(range(n_withdraw)), workers=20)

    end, data = balance(account)
    expected = start + amount * n_deposit - amount * n_withdraw
    if end != expected:
        fail(
            f"balance mismatch for {account}: start={start} end={end} "
            f"expected={expected} via replica={data.get('replica_id')}"
        )
    ok(
        f"{account}: {start} + {n_deposit} deposits - {n_withdraw} withdraws = {end} "
        f"(last replica={data.get('replica_id')})"
    )


def main() -> None:
    raise SystemExit(
        0 if run_experiment(3, "Concurrent replica operations", experiment) else 1
    )


if __name__ == "__main__":
    main()
