"""
Mini Banking Client
Connects to the server VM by real IP (NOT localhost).
Supports failover to backup server for high availability.

Put SERVER_URL or SERVER_URLS in a .env file next to this script, e.g.:
  SERVER_URL=http://192.168.56.10:8000
  # or for failover:
  SERVER_URLS=http://192.168.56.10:8000,http://192.168.56.11:8000

Usage examples (on client VM):
  python client.py balance A1001
  python client.py deposit A1001 100
  python client.py withdraw A1001 50
  python client.py demo
  python client.py stress A1001 20 10
  python client.py errors
  python client.py failover-demo A1001
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any, Optional

import requests

from envfile import load_env_file

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Prefer .env; also accept .env.client if that is what you edited
load_env_file(os.path.join(_BASE_DIR, ".env"))
load_env_file(os.path.join(_BASE_DIR, ".env.client"))

# Support multiple servers for failover
_server_urls_str = os.getenv("SERVER_URLS", "")
if _server_urls_str:
    SERVER_URLS = [url.strip().rstrip("/") for url in _server_urls_str.split(",")]
else:
    # Fallback to single SERVER_URL
    SERVER_URLS = [os.getenv("SERVER_URL", "http://127.0.0.1:8000").rstrip("/")]

# For backward compatibility
SERVER_URL = SERVER_URLS[0]


def call(
    action: str,
    account: str,
    amount: Optional[Any] = None,
    timeout: float = 10.0,
) -> dict:
    """
    Send a transaction request with automatic failover.
    Tries each server in SERVER_URLS until one responds.
    """
    payload: dict[str, Any] = {"action": action, "account": account}
    if amount is not None:
        payload["amount"] = amount

    last_error = None
    
    for server_url in SERVER_URLS:
        url = f"{server_url}/transaction"
        try:
            resp = requests.post(url, json=payload, timeout=timeout)
            
            try:
                data = resp.json()
            except ValueError:
                data = {
                    "status": "error",
                    "message": f"non-JSON response (HTTP {resp.status_code})",
                    "raw": resp.text[:300],
                }
            
            data["_http_status"] = resp.status_code
            data["_server"] = server_url  # track which server responded

            # If server is busy or in read-only / no-quorum mode (503), failover to next server
            if resp.status_code in (502, 503, 504):
                last_error = f"{server_url} returned {resp.status_code}: {data.get('message', '')}"
                continue

            return data
            
        except requests.RequestException as exc:
            last_error = f"connection failed: {exc}"
            continue  # try next server
    
    # All servers failed
    return {
        "status": "error",
        "message": f"all servers unreachable: {last_error}",
        "_attempted_servers": SERVER_URLS,
    }


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


def cmd_failover_demo(account: str) -> None:
    """
    Demonstrate automatic failover between servers.
    Keep sending requests while you manually kill Server A.
    """
    print(f"Configured servers: {SERVER_URLS}")
    print(f"\nStarting continuous requests to {account}...")
    print("Kill Server A during this test to see automatic failover.\n")
    
    for i in range(20):
        result = call("balance", account)
        server = result.get("_server", "unknown")
        status = result.get("status", "unknown")
        balance = result.get("balance", "N/A")
        
        print(f"Request {i+1:2d}: {status:8s} balance={balance:8} server={server}")
        
        if status != "success":
            print(f"           Error: {result.get('message', 'unknown')}")
        
        time.sleep(1)  # 1 second between requests
    
    print("\nFailover demo complete.")


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

    f = sub.add_parser("failover-demo", help="Demonstrate automatic failover")
    f.add_argument("account")

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    print(f"Configured servers: {SERVER_URLS}")

    # Quick connectivity check for all servers
    reachable = []
    for server_url in SERVER_URLS:
        try:
            h = requests.get(f"{server_url}/health", timeout=5)
            print(f"✓ {server_url}: HTTP {h.status_code}")
            reachable.append(server_url)
        except requests.RequestException as exc:
            print(f"✗ {server_url}: unreachable ({exc})")
    
    if not reachable and args.cmd != "failover-demo":
        print("\nERROR: No servers reachable!")
        print("Set server IPs in .env, e.g.:")
        print("  SERVER_URLS=http://192.168.56.10:8000,http://192.168.56.11:8000")
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
    elif args.cmd == "failover-demo":
        cmd_failover_demo(args.account)
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
