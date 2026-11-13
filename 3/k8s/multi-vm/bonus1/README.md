# Bonus I — multi-VM placement + worker interrupt

## Topology

| Component | Node label | Node |
|-----------|------------|------|
| `db-a` / `db-b` / `db-coord` primary | `banking-worker=w1` | `client-vm` (.102) |
| `db-*-standby-local` | same host as primary | `client-vm` |
| `db-*-standby` (remote) | `banking-worker=w2` | `server-backup` (.104) |
| bank replicas | topology spread | both workers |

Independent backups: CronJobs `backup-db-a` / `backup-db-b` → hostPath `/var/banking-backups/` on w1.

## Apply (on .101 as root)

```bash
# from Windows first:
# scp -r "...\3\k8s\multi-vm\bonus1" vboxuser@192.168.56.101:~/

export KUBECONFIG=/etc/kubernetes/admin.conf
bash /home/vboxuser/bonus1/apply-bonus1.sh
bash /home/vboxuser/bonus1/enable-replication.sh
# wait until local standbys Ready
kubectl -n banking get pods -o wide -l tier=data
kubectl -n banking exec db-a-0 -- psql -U bank -d bank -c "SELECT client_addr,state FROM pg_stat_replication;"
```

## Worker interrupt experiment

```bash
bash /home/vboxuser/bonus1/interrupt-worker.sh client-vm
```

Expect: workloads reschedule to healthy worker; balances remain valid; never two `role=primary` writers for one shard.
