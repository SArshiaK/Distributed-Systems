$ErrorActionPreference = "Stop"

$images = @(
  "2-bank-a1:latest",
  "2-bank-b1:latest",
  "2-gateway:latest",
  "2-coordinator:latest",
  "postgres:16-alpine"
)

Write-Host "==> Loading local Docker images into Minikube"
foreach ($img in $images) {
  Write-Host "  load $img"
  minikube image load $img
}

Write-Host "Done. Images are local-only (imagePullPolicy: Never)."
