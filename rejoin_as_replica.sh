#!/usr/bin/env bash
#
# rejoin_as_replica.sh
# Rebuild THIS node's PostgreSQL as a streaming replica of the current primary.
#
# Use it on Server A after it was down and Server B was promoted:
#   1. Stop the app on this node (Ctrl+C on `python server.py`).
#   2. Run:  sudo bash rejoin_as_replica.sh
#   3. Start the app again:  source venv/bin/activate && python3 server.py
#
# The script refuses to touch anything unless the peer really is a primary.
# The old data directory is kept as <PGDATA>.stale-<timestamp> for safety.

set -Eeuo pipefail

PRIMARY_HOST="${PRIMARY_HOST:-192.168.56.104}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPL_USER="${REPL_USER:-replicator}"
REPL_PASSWORD="${REPL_PASSWORD:-replicapass}"
DB_USER="${DB_USER:-bankuser}"
DB_PASSWORD="${DB_PASSWORD:-bankpass}"
DB_NAME="${DB_NAME:-minibank}"
PG_SERVICE="${PG_SERVICE:-postgresql}"

PG_VER="${PG_VER:-$(ls /etc/postgresql 2>/dev/null | sort -V | tail -n1 || true)}"
PGDATA="${PGDATA:-/var/lib/postgresql/${PG_VER}/main}"

log() { printf '\n[rejoin] %s\n' "$*"; }
die() { printf '\n[rejoin][ERROR] %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "Run as root:  sudo bash rejoin_as_replica.sh"
[ -n "$PG_VER" ] || die "Could not detect PostgreSQL version. Set PG_VER, e.g. PG_VER=18"
[ -n "$PGDATA" ] || die "Could not determine PGDATA. Set PGDATA explicitly."
command -v pg_basebackup >/dev/null 2>&1 || die "pg_basebackup not found (install postgresql-client)."

log "Target primary : $PRIMARY_HOST:$PRIMARY_PORT"
log "Local PGDATA   : $PGDATA"

# ---------------------------------------------------------------------------
# 1. Is this node already a healthy standby? Then do nothing.
# ---------------------------------------------------------------------------
LOCAL_IN_RECOVERY="$(sudo -u postgres psql -tAc 'SELECT pg_is_in_recovery()' 2>/dev/null || true)"
if [ "$LOCAL_IN_RECOVERY" = "t" ]; then
  LOCAL_STATUS="$(sudo -u postgres psql -tAc \
    'SELECT status FROM pg_stat_wal_receiver LIMIT 1' 2>/dev/null || true)"
  if [ "$LOCAL_STATUS" = "streaming" ]; then
    log "This node is already a healthy streaming standby. Nothing to do."
    exit 0
  fi
  log "This node is in recovery but not streaming (status='${LOCAL_STATUS:-none}'). Rebuilding."
fi

# ---------------------------------------------------------------------------
# 2. Verify the peer is really a primary before we destroy local data.
# ---------------------------------------------------------------------------
log "Checking that $PRIMARY_HOST is a primary..."
export PGPASSWORD="$DB_PASSWORD"
PRIMARY_IN_RECOVERY="$(psql -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$DB_USER" \
  -d "$DB_NAME" -tAc 'SELECT pg_is_in_recovery()' 2>/dev/null || true)"

[ "$PRIMARY_IN_RECOVERY" = "f" ] || die \
"Peer at $PRIMARY_HOST is not a reachable primary (pg_is_in_recovery='$PRIMARY_IN_RECOVERY').
Refusing to rebuild.
  - If the peer is still a standby, promote it first.
  - If the connection failed, make sure pg_hba.conf on $PRIMARY_HOST allows:
        host $DB_NAME $DB_USER 192.168.56.0/24 md5"

if [ "${FORCE:-0}" != "1" ]; then
  printf '\n[rejoin] This will rebuild %s from %s. Continue? [y/N] ' "$PGDATA" "$PRIMARY_HOST"
  read -r ANSWER
  case "$ANSWER" in
    y|Y|yes|YES) ;;
    *) die "Aborted by user." ;;
  esac
fi

# ---------------------------------------------------------------------------
# 3. Stop PostgreSQL and move the stale data directory aside.
# ---------------------------------------------------------------------------
log "Stopping PostgreSQL..."
systemctl stop "$PG_SERVICE" 2>/dev/null || true

STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="${PGDATA}.stale-${STAMP}"

if [ -d "$PGDATA" ]; then
  log "Keeping stale data as $BACKUP"
  mv "$PGDATA" "$BACKUP"
fi

install -d -o postgres -g postgres -m 700 "$PGDATA"

# ---------------------------------------------------------------------------
# 4. Clone the current primary.
# ---------------------------------------------------------------------------
log "Running pg_basebackup from $PRIMARY_HOST (this may take a moment)..."
export PGPASSWORD="$REPL_PASSWORD"

if ! sudo -u postgres pg_basebackup \
      -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPL_USER" \
      -D "$PGDATA" -Fp -Xs -P -R; then
  die "pg_basebackup failed.
Check on $PRIMARY_HOST:
  1. /etc/postgresql/*/main/pg_hba.conf contains:
         host replication $REPL_USER 192.168.56.101/32 md5
  2. listen_addresses = '*' and the server was restarted/reloaded.
  3. The replicator password is correct ($REPL_PASSWORD).
  4. No firewall blocks TCP $PRIMARY_PORT."
fi

# ---------------------------------------------------------------------------
# 5. Make the standby configuration explicit and deterministic.
#    (A promoted node keeps an old primary_conninfo in postgresql.auto.conf.)
# ---------------------------------------------------------------------------
AUTO_CONF="$PGDATA/postgresql.auto.conf"
touch "$AUTO_CONF"
sed -i '/^primary_conninfo[[:space:]]*=/d' "$AUTO_CONF"
cat >> "$AUTO_CONF" <<EOF
primary_conninfo = 'host=$PRIMARY_HOST port=$PRIMARY_PORT user=$REPL_USER password=$REPL_PASSWORD'
hot_standby = on
EOF
touch "$PGDATA/standby.signal"
chown postgres:postgres "$AUTO_CONF" "$PGDATA/standby.signal"
chmod 600 "$AUTO_CONF"

# ---------------------------------------------------------------------------
# 6. Start as standby and verify streaming.
# ---------------------------------------------------------------------------
log "Starting PostgreSQL as standby..."
systemctl start "$PG_SERVICE"

sleep 2
IN_RECOVERY=""
for _ in $(seq 1 20); do
  IN_RECOVERY="$(sudo -u postgres psql -tAc 'SELECT pg_is_in_recovery()' 2>/dev/null || true)"
  [ "$IN_RECOVERY" = "t" ] && break
  sleep 1
done
[ "$IN_RECOVERY" = "t" ] || die \
  "PostgreSQL did not start as a standby (pg_is_in_recovery='$IN_RECOVERY'). Check: journalctl -u $PG_SERVICE"

log "Waiting for the WAL receiver..."
STATUS=""
for _ in $(seq 1 20); do
  STATUS="$(sudo -u postgres psql -tAc \
    'SELECT status FROM pg_stat_wal_receiver LIMIT 1' 2>/dev/null || true)"
  [ "$STATUS" = "streaming" ] && break
  sleep 1
done

[ "$STATUS" = "streaming" ] || die \
"Standby is running but the WAL receiver is not streaming (status='$STATUS').
Run:  sudo -u postgres psql -c 'SELECT * FROM pg_stat_wal_receiver;'"

log "SUCCESS: this node is now a streaming replica of $PRIMARY_HOST."
log "Safety backup kept at: $BACKUP"
log "Start the app again:  source venv/bin/activate && python3 server.py"
