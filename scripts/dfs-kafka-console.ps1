#requires -version 7
<#
.SYNOPSIS
  Launch Redpanda Console against the live lab Kafka + Schema Registry, with fresh mTLS material.
.DESCRIPTION
  Reissues a short-lived (24h) Kafka mTLS client certificate into .secrets\ (the same material the
  other dfs-*.ps1 tools use), then starts Redpanda Console with scripts\redpanda-console.yaml so you
  can browse the raw oltp.* JSON topics and the curated dfs.*.changed.v1 Avro topics (decoded via the
  Schema Registry) in a browser at http://localhost:8080.

  Prerequisites: kafka-east (.21-.23) + schema-registry (.91) powered on, Vault unsealed, and the
  Redpanda Console Windows binary downloaded (pass its path via -ConsoleExe, or put it on PATH):

    $dest = "$HOME\tools\redpanda-console"
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    Invoke-WebRequest 'https://github.com/redpanda-data/console/releases/download/v3.9.0/redpanda_console_3.9.0_windows_amd64.zip' -OutFile "$dest\console.zip"
    Expand-Archive "$dest\console.zip" -DestinationPath $dest -Force
.EXAMPLE
  .\scripts\dfs-kafka-console.ps1 -ConsoleExe "$HOME\tools\redpanda-console\redpanda-console.exe"
#>
param(
    [string]$ConsoleExe = 'redpanda-console.exe'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent

$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$vault = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\vault.exe"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token

# Fresh 24h Kafka mTLS client cert into the git-ignored .secrets dir (same as dfs-curate/dfs-trace).
$sec = Join-Path $repo '.secrets'
New-Item -ItemType Directory -Force -Path $sec | Out-Null
$issued = (& $vault write -format=json pki_int/issue/kafka-broker common_name=localhost ttl=24h) | ConvertFrom-Json
$issued.data.certificate | Set-Content "$sec\kafka-client.crt" -NoNewline
$issued.data.private_key  | Set-Content "$sec\kafka-client.key" -NoNewline
Copy-Item "$HOME\.nexus\vault-ca-bundle.crt" "$sec\kafka-ca.crt" -Force

# Run from the repo root so the relative cert paths in the YAML resolve against the process CWD.
Set-Location $repo
$env:CONFIG_FILEPATH = Join-Path $repo 'scripts\redpanda-console.yaml'

Write-Host 'Starting Redpanda Console at http://localhost:8080 (Ctrl+C to stop)...' -ForegroundColor Cyan
& $ConsoleExe
