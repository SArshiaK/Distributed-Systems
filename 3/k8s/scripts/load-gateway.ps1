param(
  [string]$BaseUrl = "http://127.0.0.1:8080",
  [int]$Seconds = 90,
  [int]$Workers = 8,
  [string]$Account = "A0500",
  [ValidateSet("balance", "deposit")]
  [string]$Action = "balance"
)

$ErrorActionPreference = "Continue"
$end = (Get-Date).AddSeconds($Seconds)
$script:ok = 0
$script:fail = 0

Write-Host "Load $Action on $Account for ${Seconds}s with $Workers workers → $BaseUrl"

$jobs = 1..$Workers | ForEach-Object {
  Start-Job -ScriptBlock {
    param($BaseUrl, $Account, $Action, $End)
    $localOk = 0
    $localFail = 0
    while ((Get-Date) -lt $End) {
      try {
        $body = if ($Action -eq "deposit") {
          @{ action = "deposit"; account = $Account; amount = 1 } | ConvertTo-Json
        } else {
          @{ action = "balance"; account = $Account } | ConvertTo-Json
        }
        $null = Invoke-RestMethod "$BaseUrl/transaction" -Method POST -ContentType "application/json" -Body $body -TimeoutSec 5
        $localOk++
      } catch {
        $localFail++
      }
    }
    [pscustomobject]@{ Ok = $localOk; Fail = $localFail }
  } -ArgumentList $BaseUrl, $Account, $Action, $end
}

$results = $jobs | Wait-Job | Receive-Job
$jobs | Remove-Job -Force
$ok = ($results | Measure-Object -Property Ok -Sum).Sum
$fail = ($results | Measure-Object -Property Fail -Sum).Sum
Write-Host "done ok=$ok fail=$fail"
