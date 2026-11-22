<#
  Rapid Burst worker - one-shot Windows bootstrap (Windows Server 2019/2022, Windows 10/11 x64)

  Run in an elevated PowerShell on the Windows VPS (or Windows PC) that will host MT5:

    & ([scriptblock]::Create((irm __BACKEND_URL__/bootstrap/install.ps1))) -WorkerSecret '<WORKER_SECRET>'

  Optional:
    -AutoLogonPassword '<windows password>'   keep MT5 running after reboots (Sysinternals Autologon, LSA-encrypted)
    -Mt5SetupUrl '<broker mt5setup.exe url>'  use your broker's own MT5 installer instead of the MetaQuotes one
    -InstallDir 'C:\RapidBurst'

  Re-running the same command updates the worker code and restarts it. Nothing here places trades.
#>
param(
  [Parameter(Mandatory = $true)][string]$WorkerSecret,
  [string]$BackendUrl = "__BACKEND_URL__",
  [string]$InstallDir = "C:\RapidBurst",
  [string]$Mt5SetupUrl = "https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe",
  [string]$PythonVersion = "3.12.10",
  [string]$AutoLogonPassword = ""
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Step($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Ok($m) { Write-Host "    OK  $m" -ForegroundColor Green }

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  throw "Run this in an elevated PowerShell (Run as Administrator)."
}
if (-not [Environment]::Is64BitOperatingSystem) { throw "64-bit Windows required." }

$Py = Join-Path $InstallDir "python"
$Mt5 = Join-Path $InstallDir "mt5"
$Wk = Join-Path $InstallDir "worker"
$Data = Join-Path $InstallDir "data"
$Logs = Join-Path $InstallDir "logs"
$Tmp = Join-Path $InstallDir "tmp"
foreach ($d in @($InstallDir, $Py, $Mt5, $Wk, $Data, $Logs, $Tmp)) { New-Item -ItemType Directory -Force -Path $d | Out-Null }
$User = "$env:USERDOMAIN\$env:USERNAME"

# ---------------------------------------------------------------- 1. backend reachability + secret check
Step "Checking backend $BackendUrl"
$h = Invoke-RestMethod -Uri "$BackendUrl/healthz" -TimeoutSec 120
if (-not $h.ok) { throw "Backend not healthy" }
$zip = Join-Path $Tmp "worker.zip"
try {
  Invoke-WebRequest -Uri "$BackendUrl/bootstrap/worker.zip" -Headers @{ "X-Worker-Secret" = $WorkerSecret } -OutFile $zip -UseBasicParsing
} catch { throw "Backend rejected the worker secret (or is unreachable): $($_.Exception.Message)" }
Ok "backend reachable, worker secret accepted"

# ---------------------------------------------------------------- 2. Python
$PyExe = Join-Path $Py "python.exe"
if (-not (Test-Path $PyExe)) {
  Step "Installing Python $PythonVersion (private copy in $Py)"
  $pyInst = Join-Path $Tmp "python-$PythonVersion-amd64.exe"
  Invoke-WebRequest -Uri "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-amd64.exe" -OutFile $pyInst -UseBasicParsing
  $p = Start-Process -FilePath $pyInst -ArgumentList "/quiet InstallAllUsers=0 TargetDir=`"$Py`" Include_launcher=0 PrependPath=0 Include_test=0 Shortcuts=0 AssociateFiles=0" -Wait -PassThru
  if (-not (Test-Path $PyExe)) { throw "Python install failed (exit $($p.ExitCode))" }
}
Ok (& $PyExe --version)

# ---------------------------------------------------------------- 3. MetaTrader 5 terminal (portable)
$Term = Join-Path $Mt5 "terminal64.exe"
if (-not (Test-Path $Term)) {
  Step "Installing MetaTrader 5 terminal"
  $mtInst = Join-Path $Tmp "mt5setup.exe"
  Invoke-WebRequest -Uri $Mt5SetupUrl -OutFile $mtInst -UseBasicParsing
  Start-Process -FilePath $mtInst -ArgumentList "/auto" | Out-Null
  $found = $null
  for ($i = 0; $i -lt 120 -and -not $found; $i++) {
    Start-Sleep -Seconds 5
    $found = Get-ChildItem -Path "$env:ProgramFiles" -Filter terminal64.exe -Recurse -Depth 2 -ErrorAction SilentlyContinue |
      Where-Object { $_.LastWriteTime -gt (Get-Date).AddDays(-3650) } | Select-Object -First 1
    if ($found) { Start-Sleep -Seconds 20 }  # let the installer finish copying
  }
  if (-not $found) { throw "MT5 installer did not produce terminal64.exe within 10 minutes" }
  Get-Process terminal64, mt5setup -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
  Start-Sleep -Seconds 3
  Copy-Item -Path (Join-Path $found.DirectoryName "*") -Destination $Mt5 -Recurse -Force
  Ok "MT5 copied to $Mt5 (portable mode)"
} else { Ok "MT5 already installed at $Mt5" }
# Allow algorithmic trading in the terminal (portable config)
$cfgDir = Join-Path $Mt5 "config"
New-Item -ItemType Directory -Force -Path $cfgDir | Out-Null
$common = Join-Path $cfgDir "common.ini"
$ini = if (Test-Path $common) { Get-Content $common -Raw -Encoding Unicode } else { "" }
if ($ini -notmatch "\[Experts\]") {
  Add-Content -Path $common -Encoding Unicode -Value "`r`n[Experts]`r`nAllowLiveTrading=1`r`nAllowDllImport=0`r`nEnabled=1`r`nAccount=0`r`nProfile=0`r`n"
} else {
  $ini = $ini -replace "AllowLiveTrading=0", "AllowLiveTrading=1" -replace "(?m)^Enabled=0", "Enabled=1"
  Set-Content -Path $common -Value $ini -Encoding Unicode
}
Ok "Algo trading enabled in terminal config"

# ---------------------------------------------------------------- 4. Python packages + worker code
Step "Installing Python packages (MetaTrader5, aiohttp, cryptography, numpy)"
& $PyExe -m pip install --disable-pip-version-check -q --upgrade pip | Out-Null
& $PyExe -m pip install --disable-pip-version-check -q --upgrade MetaTrader5 aiohttp cryptography numpy
if ($LASTEXITCODE -ne 0) { throw "pip install failed" }
& $PyExe -c "import MetaTrader5 as m; print('MetaTrader5', m.__version__)"

Step "Deploying worker code"
Get-ScheduledTask -TaskName "RapidBurstWorker" -ErrorAction SilentlyContinue | Stop-ScheduledTask -ErrorAction SilentlyContinue
Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -like "*$Wk*main.py*" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Get-ChildItem $Wk -Exclude "worker.env" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Expand-Archive -Path $zip -DestinationPath $Wk -Force
$envFile = Join-Path $Wk "worker.env"
@"
BACKEND_URL=$BackendUrl
WORKER_SECRET=$WorkerSecret
MT5_PATH=$Term
MT5_PORTABLE=1
RB_DATA_DIR=$Data
"@ | Set-Content -Path $envFile -Encoding UTF8
# restrict secrets/data to this user, SYSTEM and Administrators
icacls $Data /inheritance:r /grant:r "${User}:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F" | Out-Null
icacls $envFile /inheritance:r /grant:r "${User}:F" "SYSTEM:F" "Administrators:F" | Out-Null
Ok "worker installed in $Wk"

# ---------------------------------------------------------------- 5. supervisor, watchdog, scheduled tasks
$runner = Join-Path $InstallDir "run_worker.ps1"
@"
`$ErrorActionPreference = 'Continue'
Set-Location '$Wk'
while (`$true) {
  "`$(Get-Date -Format o) starting worker" | Add-Content '$Logs\supervisor.log'
  & '$PyExe' -u '$Wk\main.py' *>> '$Logs\worker-console.log'
  "`$(Get-Date -Format o) worker exited with `$LASTEXITCODE - restarting in 5s" | Add-Content '$Logs\supervisor.log'
  Start-Sleep -Seconds 5
}
"@ | Set-Content -Path $runner -Encoding UTF8

$watch = Join-Path $InstallDir "watchdog.ps1"
@"
`$hb = '$Data\heartbeat.txt'
`$stale = `$true
if (Test-Path `$hb) { `$stale = ((Get-Date) - (Get-Item `$hb).LastWriteTime).TotalSeconds -gt 180 }
`$alive = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { `$_.CommandLine -like '*main.py*' }
if (`$stale -or -not `$alive) {
  "`$(Get-Date -Format o) watchdog restart (stale=`$stale alive=`$([bool]`$alive))" | Add-Content '$Logs\watchdog.log'
  Stop-ScheduledTask -TaskName RapidBurstWorker -ErrorAction SilentlyContinue
  `$alive | ForEach-Object { Stop-Process -Id `$_.ProcessId -Force -ErrorAction SilentlyContinue }
  Start-Sleep -Seconds 3
  Start-ScheduledTask -TaskName RapidBurstWorker
}
"@ | Set-Content -Path $watch -Encoding UTF8

Step "Registering scheduled tasks (start at logon, auto-restart, 5-minute watchdog)"
$ps = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$act = New-ScheduledTaskAction -Execute $ps -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$runner`""
$trg = New-ScheduledTaskTrigger -AtLogOn -User $User
$set = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
  -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
$prn = New-ScheduledTaskPrincipal -UserId $User -LogonType Interactive -RunLevel Highest
Register-ScheduledTask -TaskName "RapidBurstWorker" -Action $act -Trigger $trg -Settings $set -Principal $prn -Force | Out-Null

$wact = New-ScheduledTaskAction -Execute $ps -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$watch`""
$wtrg = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) -RepetitionInterval (New-TimeSpan -Minutes 5)
$wset = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName "RapidBurstWatchdog" -Action $wact -Trigger $wtrg -Settings $wset -Principal $prn -Force | Out-Null
Ok "tasks RapidBurstWorker + RapidBurstWatchdog registered"

# keep the machine awake
try { powercfg /change standby-timeout-ac 0 | Out-Null; powercfg /change hibernate-timeout-ac 0 | Out-Null } catch { }

if ($AutoLogonPassword) {
  Step "Configuring automatic Windows logon (Sysinternals Autologon, password stored as LSA secret)"
  $al = Join-Path $Tmp "AutoLogon.zip"
  Invoke-WebRequest -Uri "https://download.sysinternals.com/files/AutoLogon.zip" -OutFile $al -UseBasicParsing
  Expand-Archive -Path $al -DestinationPath (Join-Path $Tmp "autologon") -Force
  & (Join-Path $Tmp "autologon\Autologon64.exe") /accepteula $env:USERNAME $env:USERDOMAIN $AutoLogonPassword | Out-Null
  Ok "auto logon enabled for $User"
}

# ---------------------------------------------------------------- 6. start + verify
Step "Starting worker"
Start-ScheduledTask -TaskName "RapidBurstWorker"
$hb = Join-Path $Data "heartbeat.txt"
$okHb = $false
for ($i = 0; $i -lt 24; $i++) { Start-Sleep 5; if ((Test-Path $hb) -and ((Get-Date) - (Get-Item $hb).LastWriteTime).TotalSeconds -lt 30) { $okHb = $true; break } }
if ($okHb) { Ok "worker heartbeat is live" } else { Write-Warning "No heartbeat yet - check $Logs\worker-console.log" }
$h = Invoke-RestMethod -Uri "$BackendUrl/healthz" -TimeoutSec 60
if ($h.worker_online) { Ok "backend sees the worker ONLINE" } else { Write-Warning "backend does not see the worker yet (it retries automatically)" }
Remove-Item $Tmp -Recurse -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "Done. Open $BackendUrl on your iPhone, unlock, tap CONNECT MT5 and enter your DEMO login/password/server." -ForegroundColor Green
Write-Host "Logs: $Logs   Data: $Data   Tasks: RapidBurstWorker, RapidBurstWatchdog" -ForegroundColor Green
if (-not $AutoLogonPassword) { Write-Host "Note: the worker runs in your Windows session. Disconnect RDP (do not sign out). After a reboot, log in once via RDP or re-run with -AutoLogonPassword." -ForegroundColor Yellow }
