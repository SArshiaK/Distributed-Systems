$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==> Loading local images into Minikube"
& (Join-Path $here "load-local-images.ps1")

Write-Host "==> Ensuring metrics-server (for HPA)"
minikube addons enable metrics-server 2>$null | Out-Null

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

Write-Host "==> Enable Postgres replication roles (then standbys catch up)"
& (Join-Path $here "scripts\enable-replication.ps1")

Write-Host "==> Resources"
kubectl -n banking get pods,svc,sts,pvc,hpa,networkpolicy

Write-Host ""
Write-Host "If NetworkPolicy must be enforced, run once: .\scripts\enable-calico.ps1"
Write-Host "Test via Gateway:"
Write-Host "  kubectl -n banking port-forward svc/gateway 8080:8080"
Write-Host "  Invoke-RestMethod http://127.0.0.1:8080/health"
Write-Host "Core experiments: .\scripts\run-core-experiments.ps1"
