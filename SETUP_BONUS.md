# Setup Guide: Fault-Tolerant Banking Cluster

این راهنما برای بخش بونوس تمرین (۱۰٪ + ۱۵٪) است.

## معماری

```
VM1: Client
VM2: Server A + PostgreSQL (master)
VM3: Server B + PostgreSQL (replica)
VM4: Witness (arbiter)
```

---

## VM1: Client Setup

### 1. نصب dependencies
```bash
pip install -r requirements.txt
```

### 2. تنظیم `.env`
```bash
cp .env.client.example .env
```

ویرایش `.env`:
```
SERVER_URLS=http://192.168.56.10:8000,http://192.168.56.11:8000
```

### 3. تست
```bash
python client.py balance A1001
python client.py failover-demo A1001
```

---

## VM2: Server A (Primary)

### 1. نصب PostgreSQL
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

### 2. تنظیم PostgreSQL برای replication

ویرایش `/etc/postgresql/*/main/postgresql.conf`:
```
listen_addresses = '*'
wal_level = replica
max_wal_senders = 3
max_replication_slots = 3
```

ویرایش `/etc/postgresql/*/main/pg_hba.conf`:
```
# Local connections
host    minibank    bankuser    127.0.0.1/32    md5

# Replication connection from Server B
host    replication replicator  192.168.56.11/32  md5

# Client connections
host    minibank    bankuser    192.168.56.0/24   md5
```

### 3. ساخت دیتابیس و کاربران
```bash
sudo -u postgres psql
```

```sql
CREATE USER bankuser WITH PASSWORD 'bankpass';
CREATE DATABASE minibank OWNER bankuser;

-- برای replication
CREATE USER replicator WITH REPLICATION PASSWORD 'replicapass';
```

```bash
# Import schema
psql -h 127.0.0.1 -U bankuser -d minibank -f schema.sql
```

### 4. Restart PostgreSQL
```bash
sudo systemctl restart postgresql
```

### 5. تنظیم `.env` سرور
```bash
cp .env.example .env
```

ویرایش `.env`:
```
DATABASE_URL=postgresql://bankuser:bankpass@127.0.0.1:5432/minibank
MAX_CLIENTS=20
HOST=0.0.0.0
PORT=8000

# Quorum settings
ENABLE_QUORUM=true
WITNESS_URL=http://192.168.56.12:9000
PEER_SERVER_URL=http://192.168.56.11:8000
```

### 6. اجرای سرور
```bash
python server.py
```

---

## VM3: Server B (Replica)

### 1. نصب PostgreSQL
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

### 2. Stop PostgreSQL و پاک کردن data directory
```bash
sudo systemctl stop postgresql
sudo rm -rf /var/lib/postgresql/*/main
```

### 3. ساخت base backup از Server A
```bash
sudo -u postgres pg_basebackup -h 192.168.56.10 -U replicator -D /var/lib/postgresql/*/main -Fp -Xs -P -R
```
(Password: `replicapass`)

### 4. تنظیم postgresql.conf
ویرایش `/etc/postgresql/*/main/postgresql.conf`:
```
listen_addresses = '*'
hot_standby = on
```

### 5. ویرایش pg_hba.conf
```
host    minibank    bankuser    127.0.0.1/32      md5
host    minibank    bankuser    192.168.56.0/24   md5
```

### 6. Start PostgreSQL
```bash
sudo systemctl start postgresql
```

چک کردن replica status:
```bash
sudo -u postgres psql -c "SELECT * FROM pg_stat_wal_receiver;"
```

### 7. تنظیم `.env` سرور
```bash
cp .env.example .env
```

ویرایش `.env`:
```
DATABASE_URL=postgresql://bankuser:bankpass@127.0.0.1:5432/minibank
MAX_CLIENTS=20
HOST=0.0.0.0
PORT=8000

# Quorum settings
ENABLE_QUORUM=true
WITNESS_URL=http://192.168.56.12:9000
PEER_SERVER_URL=http://192.168.56.10:8000
```

### 8. اجرای سرور
```bash
python server.py
```

---

## VM4: Witness

### 1. نصب dependencies
```bash
pip install fastapi uvicorn
```

### 2. تنظیم `.env`
```bash
echo "WITNESS_HOST=0.0.0.0" > .env
echo "WITNESS_PORT=9000" >> .env
```

### 3. اجرای witness
```bash
python witness.py
```

---

## تست‌ها

### 1. تست Normal Operation
```bash
# روی Client VM
python client.py balance A1001
python client.py deposit A1001 100
python client.py balance A1001
```

### 2. تست Failover (بخش ۱۰٪)
```bash
# روی Client VM
python client.py failover-demo A1001
```

**در حین اجرا:**
- روی Server A VM: `Ctrl+C` یا `sudo systemctl stop postgresql`
- مشاهده کنید که client خودکار به Server B وصل میشه

### 3. تست Network Partition (بخش ۱۵٪)

**قطع شبکه بین Server A و Server B:**

روی Server B:
```bash
# Block Server A
sudo iptables -A INPUT -s 192.168.56.10 -j DROP
sudo iptables -A OUTPUT -d 192.168.56.10 -j DROP
```

**تست در حین partition:**
```bash
# روی Client VM
python client.py deposit A1001 100   # باید به Server A بره (موفق)
python client.py withdraw A1001 50   # Server B رد می‌کنه (no quorum)
```

**چک کردن quorum status:**
```bash
curl http://192.168.56.10:8000/quorum-status  # has_quorum: true
curl http://192.168.56.11:8000/quorum-status  # has_quorum: false, mode: read-only
```

**وصل کردن دوباره:**
```bash
# روی Server B
sudo iptables -F
```

**تست بعد از reconnect:**
```bash
python client.py balance A1001  # هر دو server یک عدد رو نشون میدن
```

---

## Troubleshooting

### PostgreSQL replication کار نمی‌کنه
```bash
# روی Server A
sudo -u postgres psql -c "SELECT * FROM pg_stat_replication;"

# روی Server B
sudo -u postgres psql -c "SELECT * FROM pg_stat_wal_receiver;"
```

### Firewall مشکل داره
```bash
# روی هر دو سرور
sudo ufw allow 8000/tcp
sudo ufw allow 5432/tcp

# روی Witness
sudo ufw allow 9000/tcp
```

### Client نمی‌تونه وصل بشه
```bash
# تست connectivity
curl http://192.168.56.10:8000/health
curl http://192.168.56.11:8000/health
curl http://192.168.56.12:9000/health
```
