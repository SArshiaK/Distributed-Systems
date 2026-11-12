$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==> Loading local images into Minikube"
& (Join-Path $here "load-local-images.ps1")

Write-Host "==> Applying banking manifests"
kubectl apply -k (Join-Path $here "base")

Write-Host "==> Waiting for databases"
kubectl -n banking rollout status statefulset/db-a --timeout=180s
kubectl -n banking rollout status statefulset/db-b --timeout=180s
kubectl -n banking rollout status statefulset/db-coord --timeout=180s

Write-Host "==> Waiting for apps"
kubectl -n banking rollout status deployment/bank-a --timeout=180s
kubectl -n banking rollout status deployment/bank-b --timeout=180s
kubectl -n banking rollout status deployment/coordinator --timeout=180s
kubectl -n banking rollout status deployment/gateway --timeout=180s

Write-Host "==> Resources"
kubectl -n banking get pods,svc,sts,pvc

Write-Host ""
Write-Host "Console UI is out of scope for now. Test via Gateway:"
Write-Host "  kubectl -n banking port-forward svc/gateway 8080:8080"
Write-Host "  curl http://127.0.0.1:8080/health"
Write-Host "  curl http://127.0.0.1:8080/shard-map"
