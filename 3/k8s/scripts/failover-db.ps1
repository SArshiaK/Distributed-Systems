param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("a", "b")]
  [string]$Shard
)

$ErrorActionPreference = "Continue"
$ns = "banking"
$primarySts = "db-$Shard"
$standbySts = "db-$Shard-standby"
$standbyPod = "$standbySts-0"
$svc = "db-$Shard-primary"

Write-Host "==> Failover shard $Shard"
Write-Host "    scale down primary $primarySts"
kubectl -n $ns scale "sts/$primarySts" --replicas=0
kubectl -n $ns wait --for=delete "pod/${primarySts}-0" --timeout=120s 2>$null

Write-Host "    promote $standbyPod"
kubectl -n $ns wait --for=condition=Ready "pod/$standbyPod" --timeout=180s
kubectl -n $ns exec $standbyPod -- psql -U bank -d bank -c "SELECT pg_promote();"

Write-Host "    label promoted standby as primary for Service $svc"
kubectl -n $ns label "pod/$standbyPod" role=primary --overwrite

$patchFile = Join-Path $env:TEMP "failover-$Shard-patch.json"
@"
{"spec":{"template":{"metadata":{"labels":{"role":"primary","shard-db":"$Shard","app":"db-$Shard-standby","tier":"data"}}}}}
"@ | Set-Content -Path $patchFile -Encoding ascii
kubectl -n $ns patch "sts/$standbySts" --type merge --patch-file $patchFile

Write-Host "    wait until primary Service has endpoints"
$deadline = (Get-Date).AddMinutes(2)
do {
  $ep = kubectl -n $ns get endpoints $svc -o jsonpath='{.subsets[*].addresses[*].ip}' 2>$null
  if ($ep) { break }
  Start-Sleep 2
} while ((Get-Date) -lt $deadline)

kubectl -n $ns get endpoints $svc 2>$null
kubectl -n $ns get pods -l "shard-db=$Shard" -o wide
Write-Host "Failover complete. Writer is now $standbyPod"
