param([string]$BaseUrl = "http://127.0.0.1:8080")
$ErrorActionPreference = "Continue"
$OutDir = Join-Path $PSScriptRoot "..\..\report\experiment-results"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$log = Join-Path $OutDir ("results-3to7-{0:yyyyMMdd-HHmmss}.txt" -f (Get-Date))
function Log([string]$m) {
  $line = "[{0:HH:mm:ss}] {1}" -f (Get-Date), $m
  Write-Host $line
  Add-Content -Path $log -Value $line
}
function Tx($body) {
  Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body ($body | ConvertTo-Json -Compress)
}
function DbBalance([string]$WriterPod, [string]$Account) {
  (kubectl -n banking exec $WriterPod -- psql -U bank -d bank -tAc "SELECT balance FROM accounts WHERE account_id='$Account';" 2>$null).Trim()
}

$aWriter = (kubectl -n banking get pods -l shard-db=a,role=primary -o jsonpath='{.items[0].metadata.name}')
$bWriter = (kubectl -n banking get pods -l shard-db=b,role=primary -o jsonpath='{.items[0].metadata.name}')
Log "writers A=$aWriter B=$bWriter log=$log"

# Append note for Exp1/2 already done
Log "NOTE Exp1/2 already recorded earlier: A0600->1010 after pod kill; A1500->1016 after B failover"

Log "--- Exp3 Concurrent Transactions ---"
$cAcc = "A0700"
$startBal = [decimal](Tx @{ action = "balance"; account = $cAcc }).balance
Log "start balance $cAcc=$startBal"
$jobs = 1..20 | ForEach-Object {
  Start-Job -ScriptBlock {
    param($BaseUrl, $Acc)
    try {
      $r = Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body (@{ action = "deposit"; account = $Acc; amount = 1 } | ConvertTo-Json) -TimeoutSec 15
      if ($r.status -eq "success") { 1 } else { 0 }
    } catch { 0 }
  } -ArgumentList $BaseUrl, $cAcc
}
$null = $jobs | Wait-Job
$okN = @($jobs | Receive-Job) | Measure-Object -Sum | Select-Object -ExpandProperty Sum
$jobs | Remove-Job -Force
$endBal = [decimal](DbBalance $aWriter $cAcc)
Log "ok_deposits=$okN db_balance=$endBal expected=$($startBal + $okN) match=$($endBal -eq ($startBal + $okN))"

Log "--- Exp4 Coordinator Interruption ---"
$reqId = [guid]::NewGuid().ToString("N")
$xferJob = Start-Job -ScriptBlock {
  param($BaseUrl, $ReqId)
  try {
    Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body (@{
      action = "transfer"; account = "A0800"; to_account = "A1800"; amount = 5; client_request_id = $ReqId
    } | ConvertTo-Json) -TimeoutSec 60
  } catch { $_.Exception.Message }
} -ArgumentList $BaseUrl, $reqId
Start-Sleep -Milliseconds 350
$coordPod = (kubectl -n banking get pods -l app=coordinator -o jsonpath='{.items[0].metadata.name}')
Log "deleting coordinator $coordPod during transfer req=$reqId"
kubectl -n banking delete pod $coordPod --wait=$false 2>$null | Out-Null
$xferRes = Receive-Job $xferJob -Wait
Remove-Job $xferJob -Force
Log "transfer result: $($xferRes | ConvertTo-Json -Compress -Depth 6)"
kubectl -n banking rollout status deploy/coordinator --timeout=120s | Out-Null
Start-Sleep -Seconds 10
try {
  $st = Invoke-RestMethod "$BaseUrl/requests/$reqId"
  Log "request status: $($st | ConvertTo-Json -Compress)"
} catch { Log "request status error: $($_.Exception.Message)" }
$a0800 = DbBalance $aWriter "A0800"
$b1800 = DbBalance $bWriter "A1800"
Log "DB A0800=$a0800 A1800=$b1800"

Log "--- Exp5 Autoscaling Under Load ---"
Log "HPA before: $(kubectl -n banking get hpa bank-a 2>$null | Out-String)"
$loadScript = Join-Path $PSScriptRoot "load-gateway.ps1"
$load = Start-Job -ScriptBlock {
  param($p, $url)
  & $p -BaseUrl $url -Seconds 80 -Workers 12 -Account "A0500" -Action "balance"
} -ArgumentList $loadScript, $BaseUrl
$deadline = (Get-Date).AddSeconds(100)
$scaled = $false
$maxSeen = 2
while ((Get-Date) -lt $deadline) {
  $cur = 0
  try { $cur = [int](kubectl -n banking get hpa bank-a -o jsonpath='{.status.currentReplicas}' 2>$null) } catch {}
  if ($cur -gt $maxSeen) { $maxSeen = $cur }
  Log "hpa bank-a replicas=$cur"
  if ($cur -gt 2) { $scaled = $true; break }
  Start-Sleep -Seconds 8
}
Receive-Job $load -Wait | ForEach-Object { Log "load: $_" }
Remove-Job $load -Force
$bal = Tx @{ action = "balance"; account = "A0500" }
Log "scaled=$scaled maxReplicasSeen=$maxSeen final A0500=$($bal | ConvertTo-Json -Compress)"
Log "HPA after:`n$(kubectl -n banking get hpa -o wide 2>$null | Out-String)"

Log "--- Exp6 Rolling Update Under Load ---"
$load2 = Start-Job -ScriptBlock {
  param($p, $url)
  & $p -BaseUrl $url -Seconds 55 -Workers 6 -Account "A0900" -Action "balance"
} -ArgumentList $loadScript, $BaseUrl
kubectl -n banking set env deploy/bank-a DEMO_ROLLING=v2
kubectl -n banking rollout status deploy/bank-a --timeout=180s
Log "rollout v2 done"
kubectl -n banking rollout undo deploy/bank-a
kubectl -n banking rollout status deploy/bank-a --timeout=180s
Log "rollback done"
Receive-Job $load2 -Wait | ForEach-Object { Log "load2: $_" }
Remove-Job $load2 -Force
$b6 = Tx @{ action = "balance"; account = "A0900" }
Log "A0900 after rolling/rollback: $($b6 | ConvertTo-Json -Compress)"
Log "history:`n$(kubectl -n banking rollout history deploy/bank-a 2>$null | Out-String)"

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
Log "A0800 $aBefore -> $aAfter ; A1800 $bBefore -> $bAfter"
Log "same_tx_id=$($r1.transaction_id -eq $r2.transaction_id) single_effect_delta_A=$($aBefore - $aAfter)"

Log "--- NetworkPolicy check ---"
Log "policies:`n$(kubectl -n banking get networkpolicy 2>$null | Out-String)"
$deny = kubectl -n banking exec deploy/gateway -- python -c "import socket;s=socket.socket();s.settimeout(2);print(s.connect_ex(('db-b-primary',5432)))" 2>$null
$allow = kubectl -n banking exec deploy/bank-b -- python -c "import socket;s=socket.socket();s.settimeout(2);print(s.connect_ex(('db-b-primary',5432)))" 2>$null
Log "gateway->db connect_ex=$deny (nonzero=denied) bank->db connect_ex=$allow (0=ok)"

Log "=== DONE === $log"
