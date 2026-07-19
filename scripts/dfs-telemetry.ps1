#requires -version 7
<#
.SYNOPSIS
  Read back the pipeline's own telemetry from ClickHouse, and prove both error paths.
.DESCRIPTION
  The curation and warehouse-sink runs record how they went — per-stage latency, end-to-end CDC lag,
  and structured errors. Those flow as JSON to the dfs.telemetry.* Kafka topics, and ClickHouse
  ingests them ITSELF through Kafka-engine tables + materialized views (ADR-0008); no .NET consumer
  sits on that path. This script reads all of it back.

  Modes:
    verify       (default) show the Kafka-engine objects, pipeline_events, cdc_lag_seconds, the
                 p50/p95/p99 latency MV, and recent error_events.
    demo-errors  emit one error down the NATIVE path (Kafka -> ClickHouse Kafka engine) and one down
                 the .NET HTTPS control path, then wait for both to land.
    all          demo-errors, then verify.

  Handles the lab plumbing: builds the root+INTERMEDIATE CA bundle ClickHouse's HTTPS listener needs,
  reads the ClickHouse admin password from Vault, and issues a short-lived Kafka mTLS client cert.

  Prerequisites: ClickHouse (ch-keeper .41-.43 + ch-shard*-rep* .44-.49) powered on and Vault
  unsealed. `demo-errors` additionally needs kafka-east (.21-.23) up, the ClickHouse-as-Kafka-client
  setup done once (handbook §1.5a), and the analytics schema migrated including Script0005.
  For rows to exist at all, run dfs-curate.ps1 (and optionally dfs-warehouse-sink.ps1) first.
.EXAMPLE
  .\scripts\dfs-telemetry.ps1
.EXAMPLE
  .\scripts\dfs-telemetry.ps1 all
#>
param(
    [ValidateSet('verify', 'demo-errors', 'all')]
    [string]$Mode = 'verify'
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent

$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$vault = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\vault.exe"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token

$sec = Join-Path $repo '.secrets'
New-Item -ItemType Directory -Force -Path $sec | Out-Null

# ClickHouse's HTTPS listener presents only its leaf, and ~/.nexus/vault-ca-bundle.crt is ROOT-only,
# so a client validating the chain needs root + intermediate concatenated (handbook §3.2 T6).
$chain = Join-Path $sec 'nexus-ca-chain.crt'
Get-Content "$HOME\.nexus\vault-ca-bundle.crt" -Raw | Set-Content $chain -NoNewline
"`n" + (& $vault read -field=certificate pki_int/cert/ca) | Add-Content $chain

$chPassword = & $vault kv get -field=password nexus/analytics/clickhouse/admin-password
$env:DFS_CLICKHOUSE_CONNECTION = "Host=192.168.70.44;Port=8443;Protocol=https;Username=admin;Password=$chPassword;Database=default"
$env:DFS_CLICKHOUSE_CACERT     = $chain
$env:DFS_CLICKHOUSE_CLUSTER    = 'nexus_analytics'

if ($Mode -in @('demo-errors', 'all')) {
    # The native leg produces to Kafka, so it needs the same 24h mTLS client cert the other tools use.
    # Extract with ConvertFrom-Json + Set-Content -NoNewline; grep/sed silently yields EMPTY files and
    # the broker then rejects the handshake with "tlsv13 alert certificate required" (§3.2 T11).
    $issued = (& $vault write -format=json pki_int/issue/kafka-broker common_name=localhost ttl=24h) | ConvertFrom-Json
    $issued.data.certificate | Set-Content "$sec\kafka-client.crt" -NoNewline
    $issued.data.private_key  | Set-Content "$sec\kafka-client.key" -NoNewline
    Copy-Item "$HOME\.nexus\vault-ca-bundle.crt" "$sec\kafka-ca.crt" -Force

    $env:DFS_KAFKA_BOOTSTRAP = '192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092'
    $env:DFS_KAFKA_CA        = "$sec\kafka-ca.crt"
    $env:DFS_KAFKA_CERT      = "$sec\kafka-client.crt"
    $env:DFS_KAFKA_KEY       = "$sec\kafka-client.key"
}

$env:GITHUB_PACKAGES_TOKEN = (gh auth token)

Write-Host "Reading pipeline telemetry from ClickHouse (mode: $Mode)..." -ForegroundColor Cyan
dotnet run --project "$repo\src\DataFlowStudio.Telemetry" -c Release -- $Mode
