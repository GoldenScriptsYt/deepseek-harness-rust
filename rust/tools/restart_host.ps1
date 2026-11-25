# Rebuild dsh-rs and (re)start its web Host on port 3192 with an isolated home.
# Unported RPCs and streams the web client asks for are logged to $env:TEMP\dsh-rs-web.err.
$ErrorActionPreference = "Continue"
$rust = Split-Path -Parent $PSScriptRoot
$repo = Split-Path -Parent $rust
Get-Process dsh-rs -ErrorAction SilentlyContinue | Stop-Process -Force
Set-Location $rust
$build = cargo build -p dsh-cli 2>&1 | Select-String "^error" -Context 0,6
if ($build) { $build; exit 1 }
$env:DSH_REPO_ROOT = $repo
$env:DSH_HOME = "$env:TEMP\dsh-rs-home"
Remove-Item "$env:TEMP\dsh-rs-web.err" -ErrorAction SilentlyContinue
Start-Process -FilePath "$rust\target\debug\dsh-rs.exe" -ArgumentList "web","--port","3192" `
  -RedirectStandardOutput "$env:TEMP\dsh-rs-web.log" -RedirectStandardError "$env:TEMP\dsh-rs-web.err" -WindowStyle Hidden
Start-Sleep -Seconds 3
Get-Content "$env:TEMP\dsh-rs-web.log"
