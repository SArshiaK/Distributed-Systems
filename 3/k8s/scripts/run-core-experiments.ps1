param(
  [string]$BaseUrl = "http://127.0.0.1:8080",
  [string]$OutDir = ""
)

$ErrorActionPreference = "Continue"
if (-not $OutDir) {
  $OutDir = Join-Path $PSScriptRoot "..\..\report\experiment-results"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$log = Join-Path $OutDir ("results-{0:yyyyMMdd-HHmmss}.txt" -f (Get-Date))
function Log([string]$m) {
  $line = "[{0:HH:mm:ss}] {1}" -f (Get-Date), $m
  Write-Host $line
  Add-Content -Path $log -Value $line
}
function Tx($body) {
  Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body ($body | ConvertTo-Json -Compress)
}
function DbBalance([string]$WriterPod, [string]$Account) {
  (kubectl -n banking exec $WriterPod -- psql -U bank -d bank -tAc "SELECT balance FROM accounts WHERE account_id='$Account';").Trim()
}
function CoordTx([string]$TxId) {
  (kubectl -n banking exec db-coord-0 -- psql -U bank -d bank -tAc "SELECT status FROM distributed_transactions WHERE transaction_id='$TxId';").Trim()
}

Log "=== CORE EXPERIMENTS log → $log ==="
Log "Gateway health: $((Invoke-RestMethod "$BaseUrl/health" | ConvertTo-Json -Compress))"

# Resolve current writers
$aWriter = (kubectl -n banking get pods -l shard-db=a,role=primary -o jsonpath='{.items[0].metadata.name}')
$bWriter = (kubectl -n banking get pods -l shard-db=b,role=primary -o jsonpath='{.items[0].metadata.name}')
Log "writers A=$aWriter B=$bWriter"

# ---- Exp 1: App Pod Failure ----
Log "--- Exp1 Application Pod Failure ---"
$before = Tx @{ action = "balance"; account = "A0600" }
Log "balance before: $($before | ConvertTo-Json -Compress)"
$pod = (kubectl -n banking get pods -l app=bank-a -o jsonpath='{.items[0].metadata.name}')
Log "deleting pod $pod"
kubectl -n banking delete pod $pod | Out-Null
Start-Sleep -Seconds 3
$during = Tx @{ action = "deposit"; account = "A0600"; amount = 10 }
Log "deposit during heal: $($during | ConvertTo-Json -Compress)"
kubectl -n banking rollout status deploy/bank-a --timeout=120s | Out-Null
$db = DbBalance $aWriter "A0600"
Log "DB A0600=$db (expect matches deposit result)"
Log "pods: $(kubectl -n banking get pods -l app=bank-a -o jsonpath='{range .items[*]}{.metadata.name}={.status.phase} {.status.containerStatuses[0].ready} {end}')"

# ---- Exp 2: Primary DB Failure (shard B) ----
Log "--- Exp2 Primary Database Failure (shard B) ---"
$bBefore = Tx @{ action = "balance"; account = "A1500" }
Log "B balance before: $($bBefore | ConvertTo-Json -Compress)"
$dep = Tx @{ action = "deposit"; account = "A1500"; amount = 15 }
Log "deposit on primary: $($dep | ConvertTo-Json -Compress)"
$standbyBal = (kubectl -n banking exec db-b-standby-0 -- psql -U bank -d bank -tAc "SELECT balance FROM accounts WHERE account_id='A1500';").Trim()
Log "standby A1500=$standbyBal"
Push-Location (Join-Path $PSScriptRoot ".")
.\failover-db.ps1 -Shard b
Pop-Location
Start-Sleep -Seconds 5
$aWriter = (kubectl -n banking get pods -l shard-db=a,role=primary -o jsonpath='{.items[0].metadata.name}')
$bWriter = (kubectl -n banking get pods -l shard-db=b,role=primary -o jsonpath='{.items[0].metadata.name}')
Log "writers after B failover A=$aWriter B=$bWriter"
$afterFail = Tx @{ action = "balance"; account = "A1500" }
Log "balance after failover: $($afterFail | ConvertTo-Json -Compress)"
Log "DB check: $(DbBalance $bWriter 'A1500')"

# ---- Exp 3: Concurrent transactions ----
Log "--- Exp3 Concurrent Transactions ---"
$cAcc = "A0700"
$startBal = [decimal](Tx @{ action = "balance"; account = $cAcc }).balance
Log "start balance $cAcc=$startBal"
$jobs = 1..20 | ForEach-Object {
  Start-Job -ScriptBlock {
    param($BaseUrl, $Acc)
    try {
      Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body (@{ action = "deposit"; account = $Acc; amount = 1 } | ConvertTo-Json) -TimeoutSec 10
      1
    } catch { 0 }
  } -ArgumentList $BaseUrl, $cAcc
}
$null = $jobs | Wait-Job
$okN = ($jobs | Receive-Job | Measure-Object -Sum).Sum
$jobs | Remove-Job -Force
$endBal = [decimal](DbBalance $aWriter $cAcc)
Log "ok_deposits=$okN db_balance=$endBal expected=$($startBal + $okN)"

# ---- Exp 4: Coordinator interruption ----
Log "--- Exp4 Coordinator Interruption ---"
$reqId = [guid]::NewGuid().ToString("N")
$xferJob = Start-Job -ScriptBlock {
  param($BaseUrl, $ReqId)
  try {
    Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body (@{
      action = "transfer"; account = "A0800"; to_account = "A1800"; amount = 5; client_request_id = $ReqId
    } | ConvertTo-Json) -TimeoutSec 60
  } catch {
    $_.Exception.Message
  }
} -ArgumentList $BaseUrl, $reqId
Start-Sleep -Milliseconds 400
$coordPod = (kubectl -n banking get pods -l app=coordinator -o jsonpath='{.items[0].metadata.name}')
Log "deleting coordinator $coordPod during transfer req=$reqId"
kubectl -n banking delete pod $coordPod --wait=false | Out-Null
$xferRes = Receive-Job $xferJob -Wait
Remove-Job $xferJob -Force
Log "transfer result: $($xferRes | ConvertTo-Json -Compress -Depth 5)"
kubectl -n banking rollout status deploy/coordinator --timeout=120s | Out-Null
Start-Sleep -Seconds 8
try {
  $st = Invoke-RestMethod "$BaseUrl/requests/$reqId"
  Log "request status: $($st | ConvertTo-Json -Compress)"
} catch {
  Log "request status error: $($_.Exception.Message)"
}
$a0800 = DbBalance $aWriter "A0800"
$b1800 = DbBalance $bWriter "A1800"
Log "DB A0800=$a0800 A1800=$b1800 (money conserved; no permanent partial)"

# ---- Exp 5: Autoscaling under load ----
Log "--- Exp5 Autoscaling Under Load ---"
Log "HPA before: $(kubectl -n banking get hpa bank-a -o jsonpath='{.status.currentReplicas}/{.spec.maxReplicas} cpu={.status.currentMetrics[0].resource.current.averageUtilization}')"
$load = Start-Job -FilePath (Join-Path $PSScriptRoot "load-gateway.ps1") -ArgumentList $BaseUrl, 75, 10, "A0500", "balance"
$deadline = (Get-Date).AddSeconds(90)
$scaled = $false
while ((Get-Date) -lt $deadline) {
  $cur = [int](kubectl -n banking get hpa bank-a -o jsonpath='{.status.currentReplicas}')
  Log "hpa bank-a replicas=$cur"
  if ($cur -gt 2) { $scaled = $true; break }
  Start-Sleep -Seconds 10
}
Receive-Job $load -Wait | Out-Null
Remove-Job $load -Force
$bal = Tx @{ action = "balance"; account = "A0500" }
Log "scaled=$scaled final balance A0500=$($bal | ConvertTo-Json -Compress)"
Log "HPA after: $(kubectl -n banking get hpa -o wide | Out-String)"

# ---- Exp 6: Rolling update under load ----
Log "--- Exp6 Rolling Update Under Load ---"
$load2 = Start-Job -FilePath (Join-Path $PSScriptRoot "load-gateway.ps1") -ArgumentList $BaseUrl, 60, 6, "A0900", "balance"
kubectl -n banking set env deploy/bank-a DEMO_ROLLING=v2
kubectl -n banking rollout status deploy/bank-a --timeout=180s
Log "rollout v2 done"
kubectl -n banking rollout undo deploy/bank-a
kubectl -n banking rollout status deploy/bank-a --timeout=180s
Log "rollback done"
Receive-Job $load2 -Wait | Out-Null
Remove-Job $load2 -Force
$b6 = Tx @{ action = "balance"; account = "A0900" }
Log "A0900 after rolling/rollback: $($b6 | ConvertTo-Json -Compress)"
Log "history: $(kubectl -n banking rollout history deploy/bank-a | Out-String)"

# ---- Exp 7: Repeated client requests ----
Log "--- Exp7 Repeated Client Requests ---"
$idem = [guid]::NewGuid().ToString("N")
$aBefore = [decimal](DbBalance $aWriter "A0800")
$bBefore = [decimal](DbBalance $bWriter "A1800")
$r1 = Tx @{ action = "transfer"; account = "A0800"; to_account = "A1800"; amount = 3; client_request_id = $idem }
$r2 = Tx @{ action = "transfer"; account = "A0800"; to_account = "A1800"; amount = 3; client_request_id = $idem }
Log "first: $($r1 | ConvertTo-Json -Compress)"
Log "repeat: $($r2 | ConvertTo-Json -Compress)"
$aAfter = [decimal](DbBalance $aWriter "A0800")
$bAfter = [decimal](DbBalance $bWriter "A1800")
Log "A0800 $aBefore -> $aAfter ; A1800 $bBefore -> $bAfter (delta should be single transfer)"
Log "tx ids equal? $($r1.transaction_id -eq $r2.transaction_id)"

# ---- NetworkPolicy check ----
Log "--- NetworkPolicy enforcement check ---"
Log "policies: $(kubectl -n banking get networkpolicy -o name | Out-String)"
$deny = kubectl -n banking exec deploy/gateway -- sh -c "nc -z -w 2 db-b-primary 5432; echo exit:$LASTEXITCODE" 2>&1
Log "gateway->db-b-primary probe: $deny"

Log "=== DONE ==="
Log "results file: $log"
