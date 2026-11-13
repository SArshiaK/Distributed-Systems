#!/usr/bin/env bash
# Bonus I: under load, drain worker w1, then recover. Run on control-plane.
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-/etc/kubernetes/admin.conf}"
WORKER="${1:-client-vm}"
NS=banking

echo "==> A0600 before"
BEFORE=$(kubectl -n "$NS" exec db-a-0 -- psql -U bank -d bank -tAc "SELECT balance FROM accounts WHERE account_id='A0600';")
echo "balance=$BEFORE"

echo "==> background load via gateway pod"
kubectl -n "$NS" exec deploy/gateway -- python -c "
import requests, time, threading, signal
stop=False
def run():
  while not stop:
    try:
      requests.post('http://127.0.0.1:8080/transaction', json={'action':'deposit','account':'A0600','amount':1}, timeout=2)
    except Exception:
      pass
    time.sleep(0.25)
t=threading.Thread(target=run, daemon=True); t.start(); time.sleep(600)
" >/tmp/bonus1-load.log 2>&1 &
LOAD_PID=$!
sleep 10

echo "==> cordon+drain $WORKER"
kubectl cordon "$WORKER"
kubectl drain "$WORKER" --ignore-daemonsets --delete-emptydir-data --force --timeout=180s || true

echo "==> during interrupt"
kubectl get nodes -o wide
kubectl -n "$NS" get pods -o wide
kubectl -n "$NS" get pods -l 'role=primary' -o wide --show-labels
kubectl -n "$NS" get endpoints db-a-primary db-b-primary

sleep 20
echo "==> uncordon recover"
kubectl uncordon "$WORKER"
kill "$LOAD_PID" 2>/dev/null || true
# stop leftover python in gateway by restarting gateway (clean)
kubectl -n "$NS" rollout restart deploy/gateway
kubectl -n "$NS" rollout status deploy/gateway --timeout=120s
sleep 15

echo "==> after"
kubectl -n "$NS" get pods -o wide
AFTER=$(kubectl -n "$NS" exec db-a-0 -- psql -U bank -d bank -tAc "SELECT balance FROM accounts WHERE account_id='A0600';" 2>/dev/null || echo "?")
echo "A0600 before=$BEFORE after=$AFTER"
echo "writers:"
kubectl -n "$NS" get pods -l 'role=primary' -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,LABELS:.metadata.labels.role --no-headers
echo "DONE"
