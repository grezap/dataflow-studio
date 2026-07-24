#requires -version 7
<#
.SYNOPSIS
  Run the DataFlow Studio pipeline with OpenTelemetry export to the lab observability tier (E16).
.DESCRIPTION
  Drives the curation drain (and, with -IncludeWarehouseSink, the StarRocks DWH load) with OTLP export
  switched on, so each engine emits distributed-tracing spans and its metrics to the lab OTel collector:

    * traces  -> the collector -> Tempo         (curation.drain / curate <entity> ; warehouse-sink.load / sink.<stage>)
    * metrics -> the collector -> Prometheus    (dfs_telemetry_emitted_total, by stream)

  The ClickHouse pipeline_events reuse each run's trace id, so a run correlates across Tempo + ClickHouse.
  It handles the lab plumbing: issues a short-lived Kafka mTLS client certificate, trusts the lab PKI
  root for the collector's server certificate (the collector is server-TLS; no client cert is needed),
  and sets the environment the engines expect. Then it prints where to inspect the result.

  Prerequisites: kafka-east + schema-registry powered on (curated/raw topics retained), the observability
  tier up (otel-collector + Tempo + Prometheus + Grafana), Prometheus launched with
  --web.enable-remote-write-receiver (obs 3E.2 fix), and Vault unsealed. For -IncludeWarehouseSink also
  power on the StarRocks shared-nothing tier. See docs/handbook.md §1.9 (OTLP demo).
.PARAMETER OtlpEndpoint
  The collector's HTTP/protobuf OTLP receiver. Use a collector IP (the leaf carries an IP SAN) from a
  WORKGROUP host that cannot resolve otel.nexus.lab. Default: https://192.168.70.182:4318.
.PARAMETER IncludeWarehouseSink
  Also run the StarRocks DWH sink (needs the StarRocks tier up + DFS_STARROCKS_CONNECTION reachable).
#>
param(
    [string]$OtlpEndpoint = 'https://192.168.70.182:4318',
    [switch]$IncludeWarehouseSink
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent

$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$vault = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\vault.exe"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token

# Fresh 24h Kafka mTLS client cert into a git-ignored .secrets dir (mirrors dfs-curate.ps1).
$sec = Join-Path $repo '.secrets'
New-Item -ItemType Directory -Force -Path $sec | Out-Null
$issued = (& $vault write -format=json pki_int/issue/kafka-broker common_name=localhost ttl=24h) | ConvertFrom-Json
$issued.data.certificate | Set-Content "$sec\kafka-client.crt" -NoNewline
$issued.data.private_key  | Set-Content "$sec\kafka-client.key" -NoNewline
Copy-Item "$HOME\.nexus\vault-ca-bundle.crt" "$sec\kafka-ca.crt" -Force

$env:DFS_KAFKA_BOOTSTRAP = '192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092'
$env:DFS_KAFKA_CA        = "$sec\kafka-ca.crt"
$env:DFS_KAFKA_CERT      = "$sec\kafka-client.crt"
$env:DFS_KAFKA_KEY       = "$sec\kafka-client.key"
$env:DFS_SR_URL          = 'https://192.168.10.91:8081'

# E16: OTLP export. The collector is server-TLS with a private-CA leaf → trust the lab PKI root (the
# collector serves its own intermediate, so the root alone completes the chain). No client cert.
$env:DFS_OTLP_ENDPOINT = $OtlpEndpoint
$env:DFS_OTLP_CACERT   = "$HOME\.nexus\vault-ca-bundle.crt"
$env:GITHUB_PACKAGES_TOKEN = (gh auth token)

Write-Host 'Curating raw CDC into curated Avro with OTLP spans + metrics ->' $OtlpEndpoint -ForegroundColor Cyan
$env:DFS_OTEL_SERVICE = 'dfs-curation'
dotnet run --project "$repo\src\DataFlowStudio.Curation" -c Release

if ($IncludeWarehouseSink) {
    # StarRocks shared-nothing FE (MySQL wire, no TLS on :9030). Password from Vault.
    $srPw = (& $vault kv get -field=password nexus/analytics/starrocks/root-password)
    $env:DFS_STARROCKS_CONNECTION = "Server=192.168.70.31;Port=9030;User ID=root;Password=$srPw;SslMode=None;AllowPublicKeyRetrieval=true"
    $env:DFS_WAREHOUSE_GROUP = 'dfs-curation-wh'   # reuse the authorized ACL prefix
    Write-Host 'Loading the StarRocks DWH with OTLP sink-stage spans...' -ForegroundColor Cyan
    $env:DFS_OTEL_SERVICE = 'dfs-warehouse-sink'
    dotnet run --project "$repo\src\DataFlowStudio.WarehouseSink" -c Release
}

Write-Host ''
Write-Host 'Inspect the telemetry:' -ForegroundColor Green
Write-Host '  Grafana    : https://192.168.70.184:3000  (Explore -> Tempo -> Service Name = dfs-curation / dfs-warehouse-sink)'
Write-Host '  Tempo API  : GET https://<tempo>:3200/api/search?tags=service.name%3Ddfs-curation  (SSH-local-curl on a tempo node)'
Write-Host '  Prometheus : dfs_telemetry_emitted_total  (metrics -> collector -> remote-write)'
