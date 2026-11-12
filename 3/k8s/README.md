# Assignment 3 — Kubernetes core (Minikube)

Namespace: `banking`

## Prerequisites

1. Docker Desktop running
2. `minikube start`
3. Local Assignment 2 images already built (Compose tags):
   - `2-bank-a1:latest` / `2-bank-b1:latest`
   - `2-gateway:latest`
   - `2-coordinator:latest`
   - `postgres:16-alpine`

Console UI from Assignment 2 is intentionally not deployed.

Manifests use these local tags with `imagePullPolicy: Never` — nothing is downloaded from GHCR.

Minikube has its own image store, so host Docker images must be loaded once:

```powershell
.\load-local-images.ps1
```

## Apply

```powershell
.\apply.ps1
# or manually:
# .\load-local-images.ps1
# kubectl apply -k .\base
kubectl -n banking get pods,svc,sts,pvc
```

Wait until databases are Ready before expecting bank pods Ready:

```powershell
kubectl -n banking rollout status sts/db-a
kubectl -n banking rollout status sts/db-b
kubectl -n banking rollout status sts/db-coord
kubectl -n banking rollout status deploy/bank-a
kubectl -n banking rollout status deploy/bank-b
kubectl -n banking rollout status deploy/gateway
```

## Access (Windows / Docker driver)

Direct `minikube ip:NodePort` often fails from the host. Use port-forward:

```powershell
kubectl -n banking port-forward svc/console 3030:3000
kubectl -n banking port-forward svc/gateway 8080:8080
```

Then open `http://127.0.0.1:3030/` and `http://127.0.0.1:8080/health`.

Or:

```powershell
minikube service console -n banking
minikube service gateway -n banking
```

Gateway NodePort is `30081` (because `30080` is already used by another Service in the cluster).

## Tear down

```powershell
kubectl delete -k .\base
```
