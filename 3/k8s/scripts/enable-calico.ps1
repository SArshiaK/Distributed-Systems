# Replace kindnet with Calico so NetworkPolicy is actually enforced.
$ErrorActionPreference = "Stop"
$manifest = Join-Path $PSScriptRoot "..\vendor\calico.yaml"
if (-not (Test-Path $manifest)) {
  New-Item -ItemType Directory -Force -Path (Split-Path $manifest) | Out-Null
  Write-Host "Downloading Calico manifest..."
  curl.exe -fsSL -o $manifest "https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml"
}

Write-Host "Applying Calico..."
kubectl apply -f $manifest

Write-Host "Removing kindnet (does not enforce NetworkPolicy)..."
kubectl -n kube-system delete daemonset kindnet --ignore-not-found

Write-Host "Waiting for calico-node..."
kubectl -n kube-system rollout status ds/calico-node --timeout=180s

Write-Host "Restarting banking workloads so new CNI attaches..."
kubectl -n banking delete pods --all --force --grace-period=0
kubectl -n banking wait --for=condition=Ready pod -l tier=app --timeout=180s
kubectl -n banking wait --for=condition=Ready pod -l tier=edge --timeout=180s
Write-Host "Calico ready. Re-run enable-replication.ps1 if standbys need re-attach."
