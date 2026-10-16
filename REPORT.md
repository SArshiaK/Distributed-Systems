# Distributed Systems Project Report: Mini Banking Transaction Server

## 1. Introduction and System Architecture

This project implements a distributed banking system supporting basic banking operations, full ACID guarantees under high concurrency, automatic failover for fault tolerance, and quorum-based split-brain prevention during network partitions.

### System Components and Network Addresses
* **Witness (Arbiter):** running on host machine at `192.168.56.103:9000`
* **Server A (Primary):** running on server VM at `192.168.56.101:8000`
* **Server B (Backup Replica):** running on second server VM at `192.168.56.104:8000`
* **Client:** running on client VM at `192.168.56.102`

```
                            +-----------------------------+
                            |     Witness (Host Node)     |
                            |       192.168.56.103        |
                            +--------------+--------------+
                                          / \
                                         /   \
                           Heartbeat    /     \   Heartbeat
                                       /       \
                                      v         v
+------------------------+      +-----------+     +-----------+
|       Client VM        | ---> | Server A  | <-> | Server B  |
|     192.168.56.102     | ---> | (Primary) |     | (Replica) |
+------------------------+      +-----+-----+     +-----+-----+
                                      |                 |
                                      | Streaming Repl  |
                                      v                 v
                                +-----------+     +-----------+
                                | Postgres  | ==> | Postgres  |
                                |  Master   |     |  Standby  |
                                +-----------+     +-----------+
```

Communication protocol is HTTP with JSON payloads, implemented with FastAPI. Persistent shared state lives in PostgreSQL.

Request format:
```json
{"action": "withdraw", "account": "A1001", "amount": 200}
```

Success response format:
```json
{"status": "success", "account": "A1001", "balance": 800}
```

---

## 2. Software Design and System Guarantees

### 2.1. ACID Properties
* **Atomicity:** Every operation runs inside `db_transaction()` using `conn.transaction()`. Any validation failure, unknown account, insufficient funds, or unexpected exception triggers automatic rollback, leaving the balance unchanged.
* **Consistency:** Database-level `CHECK (balance >= 0)` constraint plus application-level `Decimal` validation ensures amounts are always positive numbers and balances never go negative.
* **Isolation:** Write operations use `SELECT ... FOR UPDATE` row-level locking, serializing concurrent operations on the same account. This guarantees serializability with no lost updates or dirty reads. Read-only `balance` queries run without locks so they work cleanly on both primary and standby replicas.
* **Durability:** Each transaction commits to PostgreSQL WAL on disk before the HTTP success response is sent. Killing and restarting the server preserves all confirmed transactions.

### 2.2. Concurrency Control
Maximum concurrent clients is configured via the `MAX_CLIENTS` environment variable and enforced with `threading.Semaphore`. Excess clients receive HTTP 503 `server busy` without blocking the server.

### 2.3. Fault Tolerance and Automatic Failover
The client holds an ordered server list (`SERVER_URLS = [Server A, Server B]`). Requests go to Server A first. If Server A crashes, times out, or returns HTTP 502/503/504, the client automatically retries the same request on Server B within the same `call()` invocation.

Server B database runs PostgreSQL streaming replication as a hot standby. When Server B receives a write while its database is still in recovery mode, `ensure_primary_db()` promotes it via `pg_promote()` so writes succeed without manual intervention.

### 2.4. Split-Brain Prevention
A 3-node quorum model prevents divergent writes during network partitions:
```
Quorum = floor(N/2) + 1 = 2 out of 3
```
Voting members are: self, peer server, witness node.

Before every write (`deposit`/`withdraw`), `has_quorum()` checks reachability of the witness (`/ping`) and the peer (`/health`) with a 2-second timeout. A server with fewer than 2 of 3 votes enters read-only mode and rejects writes with HTTP 503 `no quorum: read-only mode`. Balance inquiries remain allowed. After the partition heals, PostgreSQL streaming replication automatically catches the standby up with missed WAL records, so both sides converge without conflicts.

---

## 3. Step-by-Step Setup and Configuration

All Python environments use `venv`. All commands below assume the code was pulled via git on each VM.

### 3.1. Witness on Host Machine (`192.168.56.103`)
In PowerShell at the project root:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install fastapi uvicorn
python witness.py
```
The witness listens on port 9000 and exposes `/health` and `/ping`.

### 3.2. Server A - Primary (`192.168.56.101`)

#### Database configuration:
```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib

sudo -u postgres psql -c "CREATE USER bankuser WITH PASSWORD 'bankpass' SUPERUSER;"
sudo -u postgres psql -c "CREATE DATABASE minibank OWNER bankuser;"
sudo -u postgres psql -c "CREATE USER replicator WITH REPLICATION PASSWORD 'replicapass';"

psql -h 127.0.0.1 -U bankuser -d minibank -f schema.sql
```

Allow replication and client connections in `/etc/postgresql/*/main/pg_hba.conf`:
```bash
sudo bash -c 'cat >> /etc/postgresql/*/main/pg_hba.conf << EOF
host    replication     replicator      192.168.56.104/32       md5
host    minibank        bankuser        192.168.56.0/24         md5
EOF'
```

Enable replication parameters in `/etc/postgresql/*/main/postgresql.conf`:
```bash
sudo bash -c 'cat >> /etc/postgresql/*/main/postgresql.conf << EOF
listen_addresses = '\''*'\''
wal_level = replica
max_wal_senders = 5
max_replication_slots = 5
EOF'

sudo systemctl restart postgresql
```

#### Application startup:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cat > .env << 'EOF'
DATABASE_URL=postgresql://bankuser:bankpass@127.0.0.1:5432/minibank
MAX_CLIENTS=20
HOST=0.0.0.0
PORT=8000
ENABLE_QUORUM=true
WITNESS_URL=http://192.168.56.103:9000
PEER_SERVER_URL=http://192.168.56.104:8000
EOF

python server.py
```

### 3.3. Server B - Backup Replica (`192.168.56.104`)

#### Database configuration as standby:
```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib

sudo systemctl stop postgresql
sudo rm -rf /var/lib/postgresql/18/main/*

PG_VER=$(ls /etc/postgresql/)
sudo -u postgres pg_basebackup -h 192.168.56.101 -U replicator -D /var/lib/postgresql/$PG_VER/main -Fp -Xs -P -R
# password: replicapass

sudo bash -c 'cat >> /etc/postgresql/*/main/postgresql.conf << EOF
listen_addresses = '\''*'\''
hot_standby = on
EOF'

sudo systemctl start postgresql
```

Verify replication from Server A:
```bash
sudo -u postgres psql -c "SELECT client_addr, state FROM pg_stat_replication;"
```
Expected: one row with `192.168.56.104 | streaming`.

#### Application startup:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cat > .env << 'EOF'
DATABASE_URL=postgresql://bankuser:bankpass@127.0.0.1:5432/minibank
MAX_CLIENTS=20
HOST=0.0.0.0
PORT=8000
ENABLE_QUORUM=true
WITNESS_URL=http://192.168.56.103:9000
PEER_SERVER_URL=http://192.168.56.101:8000
EOF

python server.py
```

### 3.4. Client VM (`192.168.56.102`)
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cat > .env << 'EOF'
SERVER_URLS=http://192.168.56.101:8000,http://192.168.56.104:8000
EOF
```

---

## 4. Test Scenarios and Expected Results

### Test 1: Normal Operations
On client VM:
```bash
python client.py balance A1001
python client.py deposit A1001 100
python client.py withdraw A1001 50
python client.py balance A1001
```
Expected: all calls return HTTP 200 with `status: success`, and the final balance equals initial + 100 - 50.

### Test 2: Error Handling
On client VM:
```bash
python client.py errors
```
Expected: all 8 error cases handled with proper codes and no server crash:
| Case | Expected |
|------|----------|
| Unknown account (Z9999) | HTTP 404 |
| Insufficient funds | HTTP 400, balance unchanged |
| Negative amount (-50) | HTTP 400 |
| Zero amount (0) | HTTP 400 |
| Invalid amount ("abc") | HTTP 400 |
| Unsupported action ("transfer") | HTTP 400 |
| Missing amount field | HTTP 400 |
| Malformed JSON | HTTP 400 |

### Test 3: Concurrency and Isolation
On client VM:
```bash
python client.py stress A1001 20 10
```
Expected: 20 concurrent threads withdraw from the same account. Final output:
```
OK — matches serial expectation
```
Final balance equals start minus (successful withdrawals x 10). No lost updates.

### Test 4: Atomicity
On client VM:
```bash
python client.py balance A1001
python client.py withdraw A1001 999999
python client.py balance A1001
```
Expected: withdrawal fails with insufficient funds, and balance is identical before and after.

### Test 5: Durability
1. On client: record balance with `python client.py balance A1001`.
2. On Server A: stop with `Ctrl+C`, then restart with `python server.py`.
3. On client: `python client.py balance A1001`.
Expected: balance identical to before the kill.

### Test 6: Automatic Failover
1. Ensure both servers and witness are running.
2. On client: `python client.py failover-demo A1001`.
3. During the 20-request loop, kill Server A with `Ctrl+C`.
Expected: requests switch from `server=http://192.168.56.101:8000` to `server=http://192.168.56.104:8000` with zero errors and zero downtime. Follow-up writes on Server B succeed:
```bash
python client.py deposit A1001 25
python client.py balance A1001
```

### Test 7: Network Partition and Split-Brain Prevention
1. Restore Server A and re-sync Server B as standby (see section 3.3) so replication is streaming again.
2. Stop `witness.py` on the host.
3. Isolate Server B (on Server B):
```bash
sudo iptables -A INPUT -s 192.168.56.101 -j DROP
sudo iptables -A OUTPUT -d 192.168.56.101 -j DROP
```
4. Check quorum from client:
```bash
curl http://192.168.56.104:8000/quorum-status
```
Expected: `{"has_quorum": false, "mode": "read-only"}`.
5. Attempt a write on the isolated server:
```bash
curl -X POST http://192.168.56.104:8000/transaction -H "Content-Type: application/json" -d '{"action":"deposit","account":"A1001","amount":50}'
```
Expected: HTTP 503 `no quorum: server in read-only mode`.
6. Balance inquiry on the isolated server still works:
```bash
curl -X POST http://192.168.56.104:8000/transaction -H "Content-Type: application/json" -d '{"action":"balance","account":"A1001"}'
```
Expected: HTTP 200 success.
7. Recover (on Server B): `sudo iptables -F`, then restart witness on host.
Expected: both servers report `has_quorum: true`, replication catches up automatically, and both servers return identical balances with no lost or duplicated transactions.

### Test 8: Logging
On Server A:
```bash
cat bank_server.log
```
Expected: every request logged with timestamp, request ID, client IP, action, account, result, and latency.
