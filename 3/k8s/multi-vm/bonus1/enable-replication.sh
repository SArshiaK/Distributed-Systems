#!/usr/bin/env bash
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-/etc/kubernetes/admin.conf}"

enable_one() {
  local pod="$1"
  echo "==> replicator on $pod"
  kubectl -n banking exec "$pod" -- psql -U bank -d bank -c \
    "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='replicator') THEN CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'repl'; ELSE ALTER ROLE replicator WITH REPLICATION LOGIN PASSWORD 'repl'; END IF; END \$\$;"
  kubectl -n banking exec "$pod" -- sh -c \
    "grep -q 'replication replicator' /var/lib/postgresql/data/pgdata/pg_hba.conf || printf '\nhost replication replicator 0.0.0.0/0 md5\nhost all replicator 0.0.0.0/0 md5\n' >> /var/lib/postgresql/data/pgdata/pg_hba.conf"
  kubectl -n banking exec "$pod" -- psql -U bank -d bank -c "SELECT pg_reload_conf();"
}

enable_one db-a-0
enable_one db-b-0
echo "DONE"
kubectl -n banking exec db-a-0 -- psql -U bank -d bank -c "SELECT client_addr,state,application_name FROM pg_stat_replication;"
kubectl -n banking exec db-b-0 -- psql -U bank -d bank -c "SELECT client_addr,state,application_name FROM pg_stat_replication;"
