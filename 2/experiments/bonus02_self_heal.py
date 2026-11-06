"""
Bonus II terminal smoke: start traffic-like requests, stop a bank replica,
wait for healer to register a replacement, confirm gateway still serves.
"""

from __future__ import annotations

import sys
import time

import requests

GATEWAY = "http://localhost:8080"
HEALER = "http://localhost:8096"


def main() -> int:
    print("== Bonus II · Self-Healing smoke ==")
    h = requests.get(f"{HEALER}/health", timeout=5)
    h.raise_for_status()
    print("healer ok")

    requests.post(f"{HEALER}/enable", json={"enabled": True}, timeout=5)
    requests.post(f"{HEALER}/traffic/start", timeout=5)
    time.sleep(1.5)

    before = requests.get(f"{HEALER}/status", timeout=5).json()
    print("before map:", before.get("shard_map"))

    crash = requests.post(
        f"{HEALER}/inject/crash",
        json={"slot_id": "A-0", "mode": "stop"},
        timeout=10,
    )
    print("crash:", crash.status_code, crash.json())

    deadline = time.time() + 90
    healed = False
    while time.time() < deadline:
        st = requests.get(f"{HEALER}/status", timeout=5).json()
        slots = (st.get("slots") or {}).get("A") or []
        a0 = next((s for s in slots if s["slot_id"] == "A-0"), None)
        if a0 and a0.get("managed") and a0.get("status") == "healthy":
            healed = True
            print("healed A-0 ->", a0.get("url"), a0.get("container_name"))
            break
        print(
            "waiting…",
            a0.get("status") if a0 else "?",
            "pending=",
            bool(st.get("pending")),
            "replacing=",
            st.get("replacing"),
        )
        time.sleep(2)

    requests.post(f"{HEALER}/traffic/stop", timeout=5)
    if not healed:
        print("FAIL: replacement did not become healthy in time")
        return 1

    # Gateway still answers on shard A
    r = requests.post(
        f"{GATEWAY}/transaction",
        json={"action": "balance", "account": "A0500"},
        timeout=8,
    )
    print("gateway balance:", r.status_code, r.json())
    if r.status_code != 200:
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
