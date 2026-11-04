# Assignment 2 — Required Experiments (1–7)

Scripts talk to the running Compose stack (`gateway:8080`, `coordinator:8090`) and use `docker` for fault injection.

## Setup

```bash
cd 2
docker compose up -d --build
pip install -r experiments/requirements.txt
```

## Run all

```bash
cd experiments
python run_all.py
```

## Run one

```bash
python run_all.py 1
python exp04_replica_interruption.py
```

## Mapping

| # | Script | Shows |
|---|--------|--------|
| 1 | `exp01_shard_distribution.py` | Accounts only on owning shard DB; gateway routes A/B correctly |
| 2 | `exp02_replica_load.py` | Many requests hit both A1 and A2 (RR) |
| 3 | `exp03_concurrent_replicas.py` | Concurrent deposit/withdraw → exact final balance |
| 4 | `exp04_replica_interruption.py` | Stop A2 under load → traffic only on A1 |
| 5 | `exp05_transfers.py` | Same-shard local tx vs cross-shard 2PC |
| 6 | `exp06_interrupted_2pc.py` | Pause after durable `COMMIT_DECIDED`, kill coordinator, recover to `COMPLETED` |
| 7 | `exp07_concurrent_cross_shard.py` | Concurrent overlapping cross-shard txs + money conservation |

Experiment 6 uses a lab fault flag file mounted at `experiments/faults/pause_after_commit_decided` (coordinator waits after durable COMMIT before sending participant commits).
