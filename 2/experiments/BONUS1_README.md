# Bonus I — Live Re-Sharding (terminal proof)

Does **not** change the Architecture Lab / Required Experiments UI.
It adds Shard C + a migrator behind compose profile `bonus`, plus this terminal demo.

## What it proves

1. Platform stays up while Shard C is added
2. Online snapshot + durable CDC catch-up while traffic continues
3. Brief fence → epoch cutover → stale writes on old owner B rejected
4. Same-shard / cross-shard traffic during migration
5. Client request IDs are idempotent
6. Optional migrator crash mid-phase + automatic resume
7. Old B data removed only after verified cutover

## Run (show this terminal to your instructor)

```powershell
cd "d:\University\3\Distributed Systems\Assignments\2"
pip install -r experiments\requirements.txt
python experiments\bonus01_live_reshard.py
```

With adversarial migrator crash during catch-up:

```powershell
python experiments\bonus01_live_reshard.py --crash-during catchup
```

Other crash points: `snapshot`, `fencing`, `activate`.

## Manual pieces (optional)

```powershell
docker compose up -d --build
docker compose --profile bonus up -d --build db-c bank-c1 bank-c2 migrator
curl -X POST http://127.0.0.1:8095/migrate/start
curl http://127.0.0.1:8095/status
```
