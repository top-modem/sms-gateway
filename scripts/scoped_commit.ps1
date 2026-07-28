param(
    [Parameter(Mandatory = $true)]
    [string]$Message,

    [Parameter(Mandatory = $true)]
    [string[]]$Files,

    [switch]$SkipChecks
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Fail([string]$msg) {
    Write-Error $msg
    exit 1
}

function Run([string]$cmd) {
    Write-Host "`n> $cmd" -ForegroundColor Cyan
    Invoke-Expression $cmd
    if ($LASTEXITCODE -ne 0) {
        Fail "Command failed: $cmd"
    }
}

# Ensure git repo root
$repoRoot = git rev-parse --show-toplevel 2>$null
if (-not $repoRoot) {
    Fail "Not inside a git repository."
}

Set-Location $repoRoot

# Block when there are already staged changes.
$staged = git diff --cached --name-only
if ($staged) {
    Write-Host "Already staged files:" -ForegroundColor Yellow
    $staged | ForEach-Object { Write-Host "  $_" }
    Fail "Please commit or unstage existing staged files before using scoped_commit.ps1"
}

# Validate file list.
foreach ($f in $Files) {
    if (-not (Test-Path $f)) {
        Fail "File not found: $f"
    }
}

# Stage only requested files.
$quoted = $Files | ForEach-Object { '"' + $_ + '"' }
Run ("git add -- " + ($quoted -join ' '))

$stagedNow = git diff --cached --name-only
if (-not $stagedNow) {
    Fail "No files were staged."
}

Write-Host "`nStaged files:" -ForegroundColor Green
$stagedNow | ForEach-Object { Write-Host "  $_" }

# Ensure staged set exactly matches requested set.
$want = $Files | ForEach-Object { $_.Replace('\\','/') } | Sort-Object -Unique
$have = $stagedNow | ForEach-Object { $_.Replace('\\','/') } | Sort-Object -Unique
$wantStr = $want -join "`n"
$haveStr = $have -join "`n"
if ($wantStr -ne $haveStr) {
    Fail "Staged files differ from requested files. Requested:`n$wantStr`nStaged:`n$haveStr"
}

if (-not $SkipChecks) {
    $needsRust = $have | Where-Object { $_ -match '\\.rs$|^Cargo\\.toml$|^Cargo\\.lock$|^src/' }
    $needsFrontend = $have | Where-Object { $_ -match '^frontend/' }

    if ($needsRust) {
        Run 'cargo check -q'
    }

    if ($needsFrontend) {
        Run 'pnpm --dir frontend build'
    }
}

Run ("git commit -m '$Message'")

Write-Host "`nCommit created successfully." -ForegroundColor Green
Write-Host "Next: run 'git push origin main' (or your target branch)." -ForegroundColor Green

