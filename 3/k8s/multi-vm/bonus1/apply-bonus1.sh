#!/usr/bin/env bash
# Run on control-plane (.101) after core banking is already up.
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-/etc/kubernetes/admin.conf}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> label workers"
kubectl label node client-vm banking-worker=w1 --overwrite
kubectl label node server-backup banking-worker=w2 --overwrite

echo "==> pin primaries to w1"
for sts in db-a db-b db-coord; do
  kubectl -n banking patch sts "$sts" --type=json -p='[
    {"op":"add","path":"/spec/template/spec/affinity","value":{
      "nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[
        {"matchExpressions":[{"key":"banking-worker","operator":"In","values":["w1"]}]}
      ]}}
    }}
  ]' || true
done

echo "==> pin remote standbys to w2"
for sts in db-a-standby db-b-standby; do
  kubectl -n banking patch sts "$sts" --type=json -p='[
    {"op":"add","path":"/spec/template/spec/affinity","value":{
      "nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[
        {"matchExpressions":[{"key":"banking-worker","operator":"In","values":["w2"]}]}
      ]}},
      "podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[
        {"labelSelector":{"matchExpressions":[
          {"key":"role","operator":"In","values":["primary"]}
        ]},"topologyKey":"kubernetes.io/hostname"}
      ]}
    }}
  ]' || true
done

echo "==> spread bank deployments across workers"
for dep in bank-a bank-b; do
  kubectl -n banking patch deploy "$dep" --type=json -p='[
    {"op":"add","path":"/spec/template/spec/topologySpreadConstraints","value":[
      {"maxSkew":1,"topologyKey":"kubernetes.io/hostname","whenUnsatisfiable":"DoNotSchedule",
       "labelSelector":{"matchLabels":{"app":"'"$dep"'"}}}
    ]}
  ]' || true
done

echo "==> apply local standbys + backup cron"
kubectl apply -f "$DIR/db-local-standby.yaml"
kubectl apply -f "$DIR/backup-cronjob.yaml"

echo "==> wait"
kubectl -n banking rollout status sts/db-a --timeout=180s || true
kubectl -n banking rollout status sts/db-b --timeout=180s || true
kubectl -n banking get pods -o wide -l tier=data
echo "DONE — next: enable replication on primaries, then wait for local standbys Ready"
