"""
Mini Banking Client
Connects to the server VM by real IP (NOT localhost).

Put SERVER_URL in a .env file next to this script, e.g.:
  SERVER_URL=http://192.168.56.10:8000

Usage examples (on client VM):
  python client.py balance A1001
  python client.py deposit A1001 100
  python client.py withdraw A1001 50
  python client.py demo
  python client.py stress A1001 20 10
  python client.py errors
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

SERVER_URL = os.getenv("SERVER_URL", "http://127.0.0.1:8000").rstrip("/")


def call(
    action: str,
    account: str,
    amount: Optional[Any] = None,
    timeout: float = 10.0,
) -> dict:
    payload: dict[str, Any] = {"action": action, "account": account}
    if amount is not None:
        payload["amount"] = amount

    url = f"{SERVER_URL}/transaction"
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        return {"status": "error", "message": f"connection failed: {exc}"}

    try:
        data = resp.json()
    except ValueError:
        return {
            "status": "error",
            "message": f"non-JSON response (HTTP {resp.status_code})",
            "raw": resp.text[:300],
        }

    data["_http_status"] = resp.status_code
    return data


def print_result(title: str, data: dict) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def cmd_balance(account: str) -> None:
    print_result("balance", call("balance", account))


def cmd_deposit(account: str, amount: float) -> None:
    print_result("deposit", call("deposit", account, amount))


def cmd_withdraw(account: str, amount: float) -> None:
    print_result("withdraw", call("withdraw", account, amount))


def cmd_demo() -> None:
    """Normal operations for the assignment video."""
    print(f"Server: {SERVER_URL}")
    print_result("1) balance A1001", call("balance", "A1001"))
    print_result("2) deposit A1001 +100", call("deposit", "A1001", 100))
    print_result("3) withdraw A1001 -50", call("withdraw", "A1001", 50))
    print_result("4) balance A1001", call("balance", "A1001"))
    print_result("5) balance A1002", call("balance", "A1002"))


def cmd_errors() -> None:
    """Exercise all required error cases."""
    print(f"Server: {SERVER_URL}")

    print_result("unknown account", call("balance", "Z9999"))
    print_result("insufficient funds", call("withdraw", "A1001", 500000))
    print_result("negative amount", call("deposit", "A1001", -50))
    print_result("zero amount", call("withdraw", "A1001", 0))
    print_result("invalid amount abc", call("deposit", "A1001", "abc"))
    print_result("unsupported action", call("transfer", "A1001", 10))

    # Missing fields
    try:
        r = requests.post(
            f"{SERVER_URL}/transaction",
            json={"action": "withdraw", "account": "A1001"},
            timeout=10,
        )
        print_result("missing amount", {"_http_status": r.status_code, **r.json()})
    except Exception as exc:
        print_result("missing amount", {"status": "error", "message": str(exc)})

    # Malformed JSON / random bytes
    try:
        r = requests.post(
            f"{SERVER_URL}/transaction",
            data=b"{not-json!!!",
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        try:
            body = r.json()
        except ValueError:
            body = {"raw": r.text[:300]}
        print_result("malformed JSON", {"_http_status": r.status_code, **body})
    except Exception as exc:
        print_result("malformed JSON", {"status": "error", "message": str(exc)})


def cmd_stress(account: str, clients: int, amount: float) -> None:
    """
    Many concurrent withdrawals on the same account.
    Final balance must equal start - (successful_withdrawals * amount).
    """
    print(f"Server: {SERVER_URL}")
    before = call("balance", account)
    print_result("balance BEFORE stress", before)
    if before.get("status") != "success":
        print("Cannot start stress test; balance inquiry failed.")
        return

    results: list[dict] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        data = call("withdraw", account, amount)
        with lock:
            results.append(data)
            status = data.get("status")
            print(f"  client-{i}: {status}  balance={data.get('balance')}  msg={data.get('message', '')}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    after = call("balance", account)
    print_result("balance AFTER stress", after)

    ok = sum(1 for r in results if r.get("status") == "success")
    fail = len(results) - ok
    start_bal = float(before["balance"])
    end_bal = float(after.get("balance", -1))
    expected = start_bal - ok * float(amount)

    print(f"\nSuccessful withdrawals: {ok}")
    print(f"Failed withdrawals:     {fail}")
    print(f"Expected final balance: {expected}")
    print(f"Actual final balance:   {end_bal}")
    print("OK — matches serial expectation" if abs(end_bal - expected) < 0.001 else "MISMATCH!")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Mini Banking Client")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("balance", help="Balance inquiry")
    b.add_argument("account")

    d = sub.add_parser("deposit", help="Deposit money")
    d.add_argument("account")
    d.add_argument("amount", type=float)

    w = sub.add_parser("withdraw", help="Withdraw money")
    w.add_argument("account")
    w.add_argument("amount", type=float)

    sub.add_parser("demo", help="Run normal operation demo")
    sub.add_parser("errors", help="Trigger all error cases")

    s = sub.add_parser("stress", help="Concurrent withdrawals on one account")
    s.add_argument("account")
    s.add_argument("clients", type=int, help="Number of concurrent clients")
    s.add_argument("amount", type=float, help="Withdraw amount per client")

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    print(f"Using SERVER_URL={SERVER_URL}")

    # Quick connectivity check
    try:
        h = requests.get(f"{SERVER_URL}/health", timeout=5)
        print(f"Health: HTTP {h.status_code} {h.text}")
    except requests.RequestException as exc:
        print(f"ERROR: cannot reach server at {SERVER_URL}")
        print(f"  {exc}")
        print("Set the real server VM IP in .env, e.g.:")
        print("  SERVER_URL=http://192.168.56.10:8000")
        return 1

    if args.cmd == "balance":
        cmd_balance(args.account)
    elif args.cmd == "deposit":
        cmd_deposit(args.account, args.amount)
    elif args.cmd == "withdraw":
        cmd_withdraw(args.account, args.amount)
    elif args.cmd == "demo":
        cmd_demo()
    elif args.cmd == "errors":
        cmd_errors()
    elif args.cmd == "stress":
        cmd_stress(args.account, args.clients, args.amount)
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
