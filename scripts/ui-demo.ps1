# ChronoDim - start the UI with demo data and smoke-test it end to end (Windows).
#
#   powershell -ExecutionPolicy Bypass -File scripts\ui-demo.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\ui-demo.ps1 -CI
#   powershell -ExecutionPolicy Bypass -File scripts\ui-demo.ps1 -Port 9000 -Data .\mydb
#
# Works in Windows PowerShell 5.1 and PowerShell 7 (pwsh).
# Requires: java 21+ on PATH. maven only if the jar isn't built yet.
# Mirror of scripts/ui-demo.sh - keep the two in sync.

param(
    [switch]$CI,
    [int]$Port = 8420,
    [string]$Data = ".\ui-demo-data"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$Jar  = "engine-cli\target\chronodim.jar"
$Base = "http://127.0.0.1:$Port"
$script:Pass = 0
$script:Fail = 0

function Say($msg)  { Write-Host ""; Write-Host "== $msg ==" -ForegroundColor White }
function Ok($msg)   { $script:Pass++; Write-Host "  PASS $msg" -ForegroundColor Green }
function Bad($msg)  { $script:Fail++; Write-Host "  FAIL $msg" -ForegroundColor Red }

function Assert-Eq($desc, $actual, $expected) {
    if ("$actual" -eq "$expected") { Ok $desc } else { Bad "$desc (got '$actual', want '$expected')" }
}
function Assert-Has($desc, $text, $needle) {
    if ("$text" -like "*$needle*") { Ok $desc } else { Bad "$desc (no '$needle' in response)" }
}

# HTTP status code that also works for 4xx/5xx on Windows PowerShell 5.1.
function Get-StatusCode($method, $url, $body) {
    try {
        if ($method -eq "POST") {
            $r = Invoke-WebRequest -Uri $url -Method Post -Body $body -UseBasicParsing
        } else {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing
        }
        return [int]$r.StatusCode
    } catch {
        if ($_.Exception.Response) { return [int]$_.Exception.Response.StatusCode } else { throw }
    }
}

Say "1/5 build"
if (-not (Test-Path $Jar)) {
    Write-Host "  jar missing - building (first time only)..."
    & mvn -q -DskipTests package
    if ($LASTEXITCODE -ne 0) { Write-Host "maven build failed" -ForegroundColor Red; exit 1 }
}
Write-Host "  using $Jar"

Say "2/5 seed demo data in $Data"
function Run-Chronodim {
    # Local EAP: under 'Stop', PS 5.1 turns redirected native stderr into a
    # terminating error even when the command succeeds (java notes, warnings).
    $ErrorActionPreference = "Continue"
    & java -jar $Jar @args 2>$null
    if ($LASTEXITCODE -ne 0) { throw "chronodim $($args -join ' ') failed (exit $LASTEXITCODE)" }
}
if (-not (Test-Path $Data)) {
    Run-Chronodim table create -f examples\customer.yaml -d $Data | Out-Null
    Run-Chronodim apply examples\changes.json -d $Data -t customer --load-id demo-day1 --json | Out-Null
    Run-Chronodim apply examples\changes.csv  -d $Data -t customer --load-id demo-day2 --json | Out-Null
    Write-Host "  created table 'customer' + 2 demo loads"
} else {
    Write-Host "  reusing existing $Data"
}

Say "3/5 start UI on port $Port"
$OutLog = "$Data-ui.log"
$ErrLog = "$Data-ui.err.log"
$Ui = Start-Process -FilePath "java" -ArgumentList "-jar", $Jar, "ui", "-d", $Data, "--port", "$Port" `
        -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog -NoNewWindow -PassThru

function Stop-Ui {
    if ($Ui -and -not $Ui.HasExited) {
        Stop-Process -Id $Ui.Id -Force -ErrorAction SilentlyContinue
        $Ui.WaitForExit()   # release the port before a follow-up run binds it
    }
}

$ready = $false
for ($i = 0; $i -lt 50; $i++) {
    try {
        Invoke-RestMethod -Uri "$Base/api/stats" -TimeoutSec 2 | Out-Null
        $ready = $true; break
    } catch {
        if ($Ui.HasExited) {
            Write-Host "UI process died - $OutLog / $ErrLog :" -ForegroundColor Red
            Get-Content $OutLog, $ErrLog -ErrorAction SilentlyContinue | Select-Object -Last 5
            exit 1
        }
        Start-Sleep -Milliseconds 200
    }
}
if (-not $ready) { Write-Host "UI never became ready" -ForegroundColor Red; Stop-Ui; exit 1 }
Write-Host "  up: $Base (pid $($Ui.Id), log $OutLog)"

try {
    Say "4/5 smoke tests"

    $page = (Invoke-WebRequest -Uri "$Base/" -UseBasicParsing).Content
    Assert-Has "console page serves" $page "ChronoDim Console"

    $tables = Invoke-WebRequest -Uri "$Base/api/tables" -UseBasicParsing
    Assert-Has "catalog lists 'customer'" $tables.Content '"table":"customer"'

    $rows = Invoke-RestMethod -Uri "$Base/api/tables/customer/rows?limit=50"
    Assert-Eq "current rows = 2 (C1, C4)" (@($rows.rows).Count) 2

    $asof = Invoke-RestMethod -Uri "$Base/api/tables/customer/rows?as_of=2026-07-02T00:00:00Z"
    $asofIds = @($asof.rows | ForEach-Object { $_.row.customer_id })
    Assert-Has "time travel sees deleted C2" ($asofIds -join ",") "C2"

    $hist = Invoke-RestMethod -Uri "$Base/api/tables/customer/history?customer_id=C1"
    $h = @($hist.versions).Count
    if ($h -ge 2) { Ok "C1 has history ($h versions)" } else { Bad "C1 history (got $h versions)" }

    # Values must differ from whatever a previous run stored, or the engine
    # (correctly) reports a no-op instead of an update - salt them per run.
    $epoch = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $load  = "script-$epoch"
    $salt  = $epoch % 500
    $body  = '[{"customer_id":"C1","name":"Alice ' + $load + '","segment":"RETAIL","risk_score":' + $salt + '.5,"exposure":"11500.00","updated_at":"2026-07-12T09:00:00Z"}]'
    $apply = Invoke-RestMethod -Uri "$Base/api/tables/customer/apply?load_id=$load" -Method Post -Body $body
    Assert-Eq "apply via API counts 1 update" $apply.tables[0].updates 1

    $dupBody = '[{"customer_id":"C1","name":"EVIL OVERWRITE","segment":"RETAIL","risk_score":1.0,"exposure":"1.00","updated_at":"2026-07-12T10:00:00Z"}]'
    $dup = Invoke-RestMethod -Uri "$Base/api/tables/customer/apply?load_id=$load" -Method Post -Body $dupBody
    Assert-Eq "duplicate load_id not re-applied" $dup.already_applied "True"

    $cur = Invoke-RestMethod -Uri "$Base/api/tables/customer/history?customer_id=C1"
    Assert-Has "stored row is the winner's" $cur.versions[0].row.name "Alice $load"

    $badBody = '[{"customer_id":"C9","name":"Zed","segment":"NOPE","risk_score":1.0,"exposure":"1.00","updated_at":"2026-07-12T11:00:00Z"}]'
    $badRow = Invoke-RestMethod -Uri "$Base/api/tables/customer/apply?load_id=$load-bad" -Method Post -Body $badBody
    Assert-Eq "quality gate rejects bad segment" $badRow.tables[0].rejects 1

    $man = Invoke-WebRequest -Uri "$Base/api/manifests?table=customer&limit=100" -UseBasicParsing
    Assert-Has "audit trail has our load" $man.Content "`"load_id`":`"$load`""

    $verify = Invoke-RestMethod -Uri "$Base/api/verify"
    if ($verify.state_fingerprint) { Ok "verify fingerprint: $($verify.state_fingerprint)" } else { Bad "verify returned no fingerprint" }

    $badCfg = Get-StatusCode "POST" "$Base/api/tables" "table: 1 bad"
    Assert-Eq "bad config rejected with 400" $badCfg 400
}
catch {
    Bad "unexpected error: $($_.Exception.Message)"
}

Say "5/5 result: $($script:Pass) passed, $($script:Fail) failed"
if ($script:Fail -gt 0) {
    Stop-Ui
    exit 1
}
if ($CI) {
    Write-Host "CI mode - stopping UI."
    Stop-Ui
} else {
    Write-Host "UI is RUNNING -> $Base   (stop with: Stop-Process -Id $($Ui.Id))"
    Write-Host "Try: Data tab as-of 2026-07-02T00:00:00Z -> deleted customer C2 is visible in the past."
}
exit 0
