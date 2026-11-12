# Kubernetes core (Minikube) — fault-tolerant banking

Namespace: `banking`

## Prerequisites

1. Docker Desktop running
2. `minikube start`
3. Local images already built:
   - `2-bank-a1:latest` / `2-bank-b1:latest`
   - `2-gateway:latest`
   - `2-coordinator:latest`
   - `postgres:16-alpine`

Manifests use `imagePullPolicy: Never`. Load host images into Minikube once:

```powershell
.\load-local-images.ps1
```

## Apply

```powershell
.\apply.ps1
```

For **enforced** NetworkPolicy (Calico instead of kindnet), run once:

```powershell
.\scripts\enable-calico.ps1
.\scripts\enable-replication.ps1
```

## Access (Windows / Docker driver)

Prefer port-forward:

```powershell
kubectl -n banking port-forward svc/gateway 8080:8080
Invoke-RestMethod http://127.0.0.1:8080/health
```

Gateway NodePort is `30081`.

## HA / experiments

```powershell
.\scripts\enable-replication.ps1
.\scripts\failover-db.ps1 -Shard b
.\scripts\load-gateway.ps1 -Seconds 60 -Workers 8
.\scripts\run-core-experiments.ps1
```

## Tear down

```powershell
kubectl delete -k .\base
```
