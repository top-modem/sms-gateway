param(
    [Parameter(Mandatory = $true)][string]$Root,
    [Parameter(Mandatory = $true)][string]$Frontend,
    [Parameter(Mandatory = $true)][string]$Profile,
    [Parameter(Mandatory = $true)][string]$Jobs
)

$ErrorActionPreference = "Stop"
$logDir = Join-Path $Root "target"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$feOut = Join-Path $logDir "build_frontend.log"
$feErr = Join-Path $logDir "build_frontend.err.log"
$beOut = Join-Path $logDir "build_backend.log"
$beErr = Join-Path $logDir "build_backend.err.log"

$fe = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "pnpm run build" `
    -WorkingDirectory $Frontend -NoNewWindow -PassThru `
    -RedirectStandardOutput $feOut -RedirectStandardError $feErr
$be = Start-Process -FilePath "cargo" -ArgumentList "build", "--profile", $Profile, "-j", $Jobs `
    -WorkingDirectory $Root -NoNewWindow -PassThru `
    -RedirectStandardOutput $beOut -RedirectStandardError $beErr
# ExitCode is only populated if the handle was opened before the process exits.
$null = $fe.Handle
$null = $be.Handle

$fe.WaitForExit()
$be.WaitForExit()

Write-Host "----- frontend (exit $($fe.ExitCode)) -----"
Get-Content $feOut, $feErr -ErrorAction SilentlyContinue
Write-Host "----- backend (exit $($be.ExitCode)) -----"
Get-Content $beOut, $beErr -ErrorAction SilentlyContinue

if ($fe.ExitCode -ne 0) { Write-Host "Frontend build failed."; exit 1 }
if ($be.ExitCode -ne 0) { Write-Host "Backend build failed."; exit 1 }
exit 0
