$ErrorActionPreference = "Continue"

function Enable-Primary([string]$Pod) {
  $exists = kubectl -n banking get pod $Pod -o name 2>$null
  if (-not $exists) {
    Write-Host "==> skip $Pod (not found)"
    return
  }
  Write-Host "==> Enabling replication on $Pod"
  kubectl -n banking wait --for=condition=Ready "pod/$Pod" --timeout=180s | Out-Null

  kubectl -n banking exec $Pod -- psql -U bank -d bank -c "DO `$`$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'repl'; ELSE ALTER ROLE replicator WITH REPLICATION LOGIN PASSWORD 'repl'; END IF; END `$`$;"

  kubectl -n banking exec $Pod -- sh -c "grep -q 'replication replicator' /var/lib/postgresql/data/pgdata/pg_hba.conf || printf '\nhost replication replicator 0.0.0.0/0 md5\nhost replication replicator ::0/0 md5\nhost all replicator 0.0.0.0/0 md5\n' >> /var/lib/postgresql/data/pgdata/pg_hba.conf"

  kubectl -n banking exec $Pod -- psql -U bank -d bank -c "SELECT pg_reload_conf();"
  Write-Host "    ok"
}

# Only configure writers (standbys are read-only until promoted).
$writers = @(
  (kubectl -n banking get pods -l "shard-db=a,role=primary" -o jsonpath='{.items[0].metadata.name}' 2>$null),
  (kubectl -n banking get pods -l "shard-db=b,role=primary" -o jsonpath='{.items[0].metadata.name}' 2>$null)
) | Where-Object { $_ }

if (-not $writers -or $writers.Count -eq 0) {
  Enable-Primary "db-a-0"
  Enable-Primary "db-b-0"
} else {
  foreach ($w in $writers) { Enable-Primary $w }
}
Write-Host "Done."

