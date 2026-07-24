#requires -version 7
<#
.SYNOPSIS
  Run the DataFlow Studio pipeline with OpenLineage emission to Marquez (E16), then read the lineage
  graph back — proving the "if I change this source, what breaks downstream?" view on live lab data.
.DESCRIPTION
  Runs the curation drain (raw CDC → curated Avro) and, unless -SkipWarehouseSink, the StarRocks DWH
  load, both with OpenLineage on. Each engine emits a job run (START then COMPLETE) to Marquez:

      oltp.OltpDb.dbo.*  --[curation]-->  dfs.*.changed.v1  --[warehouse-sink]-->  dwh.dim_*/fact_*

  Each run's OpenLineage runId is the run's OpenTelemetry trace id, so a run is one correlated entity
  across Tempo (traces), ClickHouse (pipeline_events), and Marquez (lineage). Then it reads the graph
  back through the Marquez REST API (jobs + datasets + downstream edges from a raw topic).

  Emission is from the build host straight to the Marquez front door by IP, validating the private-CA
  leaf against the lab PKI root (the same custom-root trust the OTLP exporter uses; server-TLS only, no
  client cert). The read-back is SSH-local-curl on the marquez node against its own CA.
.PARAMETER MarquezEndpoint
  The Marquez base URL (nginx TLS front door). Default https://192.168.70.127 (the .127 IP SAN lets a
  WORKGROUP host validate it; otel/marquez DNS names do not resolve here).
.PARAMETER Namespace
  The OpenLineage namespace. Default 'dataflow-studio'.
.PARAMETER SkipWarehouseSink
  Only run curation (the raw → curated leg). Omit to also load StarRocks (the curated → DWH leg).
#>
param(
    [string]$MarquezEndpoint = 'https://192.168.70.127',
    [string]$Namespace = 'dataflow-studio',
    [switch]$SkipWarehouseSink
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

# E16: OpenLineage → Marquez. Trust the lab PKI root for the front-door leaf (server-TLS only, no client
# cert; the leaf carries an IP SAN so the base URL can be the collector IP).
$env:DFS_MARQUEZ_ENDPOINT  = $MarquezEndpoint
$env:DFS_MARQUEZ_CACERT    = "$HOME\.nexus\vault-ca-bundle.crt"
$env:DFS_MARQUEZ_NAMESPACE = $Namespace
$env:GITHUB_PACKAGES_TOKEN = (gh auth token)

Write-Host "Curating raw CDC → curated Avro, emitting OpenLineage → $MarquezEndpoint (namespace '$Namespace')..." -ForegroundColor Cyan
dotnet run --project "$repo\src\DataFlowStudio.Curation" -c Release

if (-not $SkipWarehouseSink) {
    $srPw = (& $vault kv get -field=password nexus/analytics/starrocks/root-password)
    $env:DFS_STARROCKS_CONNECTION = "Server=192.168.70.31;Port=9030;User ID=root;Password=$srPw;SslMode=None;AllowPublicKeyRetrieval=true"
    $env:DFS_WAREHOUSE_GROUP = 'dfs-curation-wh'   # reuse the authorized ACL prefix
    Write-Host 'Loading the StarRocks DWH, emitting the curated → DWH lineage...' -ForegroundColor Cyan
    dotnet run --project "$repo\src\DataFlowStudio.WarehouseSink" -c Release
}

# ── Read the lineage graph back from Marquez (SSH-local-curl on the node against its own CA) ──────────
Write-Host "`n=== Reading the lineage graph back from Marquez ===" -ForegroundColor Cyan
$sshOpts = @('-i', "$HOME\.ssh\nexus_gateway_ed25519", '-o', 'ConnectTimeout=10', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no')
$appIp = ([uri]$MarquezEndpoint).Host
$readback = @"
set -euo pipefail
NS='$Namespace'
CA=/etc/ssl/certs/platform-tools-ca.pem   # world-readable copy of the platform-tools CA (nexusadmin can read it)
CURL="curl -sS --max-time 20 --cacert `$CA --resolve marquez.nexus.lab:443:127.0.0.1"
API="https://marquez.nexus.lab/api/v1"
echo '--- jobs in the namespace ---'
`$CURL "`$API/namespaces/`$NS/jobs" | grep -o '"name":"[^"]*"' | sort -u
echo '--- datasets in the namespace ---'
`$CURL "`$API/namespaces/`$NS/datasets" | grep -o '"name":"[^"]*"' | sort -u
echo '--- downstream of oltp.OltpDb.dbo.Customers (what breaks if it changes) ---'
`$CURL "`$API/lineage?nodeId=dataset:`$NS:oltp.OltpDb.dbo.Customers&depth=10" | grep -o '"name":"[^"]*"' | sort -u
echo READBACK_DONE
"@
$rbOut = ($readback -replace "`r`n","`n") | & ssh @sshOpts "nexusadmin@$appIp" "tr -d '\r' | bash -s" 2>&1 | Out-String
Write-Host $rbOut.Trim()
if ($rbOut -notmatch 'READBACK_DONE') { throw "lineage read-back failed:`n$rbOut" }

Write-Host "`nOpen the graph in a browser: $MarquezEndpoint  (namespace '$Namespace')" -ForegroundColor Green
