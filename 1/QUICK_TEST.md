# Quick Test Guide - Bonus Sections

## بخش ۱۰٪: Fault Tolerance (Failover)

### Setup سریع (۳ VM):
1. **VM1 (Client)**: تنظیم `.env` با `SERVER_URLS=http://192.168.56.10:8000,http://192.168.56.11:8000`
2. **VM2 (Server A)**: PostgreSQL master + `python server.py`
3. **VM3 (Server B)**: PostgreSQL replica + `python server.py`

### تست:
```bash
# Client VM
python client.py failover-demo A1001
```

**در حین اجرا Server A رو kill کن:**
```bash
# Server A VM
Ctrl+C
```

**نتیجه مورد انتظار:**
- Request‌ها خودکار به Server B میرن
- هیچ transaction گم نمیشه
- Balance صحیح میمونه

### بازگرداندن Server A به‌عنوان replica (بدون قطعی replication)

پس از اینکه B ارتقا یافت، A دیگر Master معتبر نیست. برنامه به‌طور خودکار این را
تشخیص می‌دهد و روی A همهٔ درخواست‌ها را با `503 stale primary` رد می‌کند.

```bash
# روی Server B — یک‌بار: اجازهٔ replication به A
sudo bash -c 'cat >> /etc/postgresql/*/main/pg_hba.conf << EOF
host    replication     replicator      192.168.56.101/32       md5
host    minibank        bankuser        192.168.56.0/24         md5
EOF'
sudo systemctl reload postgresql

# روی Server A — برنامه را ببند، سپس:
sudo bash rejoin_as_replica.sh
```

اسکریپت به‌طور خودکار:
1. تأیید می‌کند B واقعاً primary است،
2. دیتابیس قدیمی A را با نام `<PGDATA>.stale-<timestamp>` کنار می‌گذارد،
3. با `pg_basebackup -R` از B کلون می‌گیرد،
4. A را به‌صورت standby بالا می‌آورد و منتظر `streaming` می‌ماند.

بررسی نهایی:

```bash
# روی Server A
sudo -u postgres psql -tAc "SELECT pg_is_in_recovery()"      # باید t باشد
sudo -u postgres psql -c "SELECT status FROM pg_stat_wal_receiver;"  # streaming

# روی Server B
sudo -u postgres psql -c "SELECT client_addr, state FROM pg_stat_replication;"
# باید 192.168.56.101 | streaming باشد

# سپس برنامه را دوباره روی A بالا بیاور
source venv/bin/activate && python3 server.py
```

---

## بخش ۱۵٪: Network Partition (Split Brain Prevention)

### Setup سریع (۴ VM):
1. **VM1 (Client)**: همون بالا
2. **VM2 (Server A)**: `.env` با `ENABLE_QUORUM=true`, `WITNESS_URL=http://192.168.56.12:9000`
3. **VM3 (Server B)**: `.env` با `ENABLE_QUORUM=true`, `WITNESS_URL=http://192.168.56.12:9000`
4. **VM4 (Witness)**: `python witness.py`

### تست Network Partition:

#### مرحله ۱: قطع شبکه
```bash
# Server B VM
sudo iptables -A INPUT -s 192.168.56.10 -j DROP
sudo iptables -A OUTPUT -d 192.168.56.10 -j DROP
```

#### مرحله ۲: بررسی وضعیت
```bash
# Client VM
curl http://192.168.56.10:8000/quorum-status
# نتیجه: {"has_quorum": true, "mode": "primary"}

curl http://192.168.56.11:8000/quorum-status
# نتیجه: {"has_quorum": false, "mode": "read-only"}
```

#### مرحله ۳: تست write operations
```bash
# Client VM
python client.py deposit A1001 100
# موفق - به Server A میره

# به Server B مستقیم بزن (فرض کن CLIENT_URL تنظیم شده)
curl -X POST http://192.168.56.11:8000/transaction \
  -H "Content-Type: application/json" \
  -d '{"action":"deposit","account":"A1001","amount":50}'
# خطا: "no quorum: server in read-only mode"
```

#### مرحله ۴: تست balance inquiry (read-only)
```bash
# از Server B
curl -X POST http://192.168.56.11:8000/transaction \
  -H "Content-Type: application/json" \
  -d '{"action":"balance","account":"A1001"}'
# موفق - balance inquiry در read-only mode مجازه
```

#### مرحله ۵: وصل کردن دوباره
```bash
# Server B VM
sudo iptables -F
```

#### مرحله ۶: بررسی consistency
```bash
# Client VM
python client.py balance A1001
# هر دو server باید یک balance نشون بدن (sync شده از A به B)
```

---

## بخش اختیاری: Lease (جلوگیری کامل از Split-Brain)

این حالت سوراخ «شمارش رأی» را می‌بندد. اگر فقط لینک A↔B قطع شود ولی هر دو به Witness
دسترسی داشته باشند، در حالت Quorum هر دو خود را primary می‌دانند. با Lease فقط
یک سرور در هر لحظه رهبر است.

### تنظیم (روی هر دو سرور در `.env`):

```bash
ENABLE_LEASE=true
NODE_ID=server-a        # روی Server B: server-b
LEASE_TTL=6
LEASE_RENEW_INTERVAL=2
```

سپس هر دو سرور را ری‌استارت کنید. Witness نیازی به تنظیم اضافه ندارد.

### تست ۱: بررسی رهبر

```bash
curl http://192.168.56.101:8000/quorum-status
# انتظار: "lease": {"is_leader": true, "leader": "server-a", ...}

curl http://192.168.56.104:8000/quorum-status
# انتظار: "lease": {"is_leader": false, "leader": "server-a", ...}, mode=read-only
```

### تست ۲: قطع فقط لینک A↔B (هر دو به Witness وصل‌اند)

```bash
# روی Server B
sudo iptables -A INPUT  -s 192.168.56.101 -j DROP
sudo iptables -A OUTPUT -d 192.168.56.101 -j DROP
```

نتیجه مورد انتظار: Server A همچنان می‌نویسد، اما Server B با وجود داشتن
`has_quorum=true` به خاطر نداشتن lease نوشتن را رد می‌کند:

```bash
curl -X POST http://192.168.56.104:8000/transaction \
  -H "Content-Type: application/json" \
  -d '{"action":"deposit","account":"A1001","amount":50}'
# خطا: "not primary: server in read-only mode (lease not held)"
```

### تست ۳: مردن Server A

Server A را با `Ctrl+C` ببندید. حداکثر بعد از `LEASE_TTL` ثانیه، Server B
lease را می‌گیرد و primary می‌شود:

```bash
curl http://192.168.56.104:8000/quorum-status
# انتظار: "lease": {"is_leader": true, "leader": "server-b", ...}, mode=primary
```

### پاک‌سازی

```bash
# روی Server B
sudo iptables -F
```

> نکته: اگر Witness خاموش شود، پس از پایان `LEASE_TTL` هیچ سروری نمی‌تواند
> بنویسد (رفتار Fail-Closed). این انتخاب عمدی برای حفظ صحت داده است.

---

## چک‌لیست برای ویدیو

### بخش ۱۰٪:
- [ ] هر دو server run شدن
- [ ] Client به هر دو وصل میشه
- [ ] `failover-demo` اجرا شد
- [ ] Server A kill شد در حین requests
- [ ] Client خودکار به Server B وصل شد
- [ ] Balance صحیح موند

### بخش ۱۵٪:
- [ ] سه VM (A, B, Witness) run شدن
- [ ] Quorum status نشون داده شد
- [ ] شبکه قطع شد (iptables)
- [ ] Server A همچنان write می‌کنه
- [ ] Server B write رو رد می‌کنه (read-only)
- [ ] Server B همچنان balance inquiry قبول می‌کنه
- [ ] شبکه وصل شد
- [ ] هر دو server sync شدن
- [ ] هیچ transaction گم نشده یا دوبار execute نشده

---

## نکات مهم

1. **PostgreSQL Replication**: حتماً check کن که replica کار می‌کنه:
   ```bash
   # Server A
   sudo -u postgres psql -c "SELECT * FROM pg_stat_replication;"
   
   # Server B
   sudo -u postgres psql -c "SELECT * FROM pg_stat_wal_receiver;"
   ```

2. **Quorum Timeout**: اگه شبکه کنده، 2 ثانیه طول می‌کشه تا quorum check fail بشه.

3. **Read-Only Mode**: در partition، Server B فقط `balance` قبول می‌کنه، نه `deposit`/`withdraw`.

4. **Witness Role**: Witness فقط برای voting هست، دیتا نداره.

5. **Client Failover**: Client خودکار به server بعدی می‌ره، نیاز به manual intervention نیست.
