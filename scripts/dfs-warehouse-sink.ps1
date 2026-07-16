#requires -version 7
<#
.SYNOPSIS
  Load the StarRocks Kimball DWH from the curated Avro topics (one drain pass).
.DESCRIPTION
  Runs the DataFlowStudio.WarehouseSink tool: consumes the curated dfs.*.changed.v1 topics, SCD2-loads
  the dimensions, reloads the facts, and prints a per-entity count. Handles the lab plumbing: issues a
  short-lived Kafka mTLS client cert, reads the StarRocks root password from Vault, and sets the
  environment the sink expects.

  Prerequisites: kafka-east + schema-registry powered on with the curated topics populated (run
  dfs-curate.ps1 first), the StarRocks shared-nothing tier powered on with the dwh schema migrated
  (Migrations.Starrocks), and Vault unsealed.
#>
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent

$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$vault = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\vault.exe"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token

# Fresh 24h Kafka mTLS client cert into a git-ignored .secrets dir.
$sec = Join-Path $repo '.secrets'
New-Item -ItemType Directory -Force -Path $sec | Out-Null
$issued = (& $vault write -format=json pki_int/issue/kafka-broker common_name=localhost ttl=24h) | ConvertFrom-Json
$issued.data.certificate | Set-Content "$sec\kafka-client.crt" -NoNewline
$issued.data.private_key  | Set-Content "$sec\kafka-client.key" -NoNewline
Copy-Item "$HOME\.nexus\vault-ca-bundle.crt" "$sec\kafka-ca.crt" -Force

# StarRocks root password (MySQL wire on the FE is TLS-off — SslMode=None).
$sr = (& $vault kv get -field=password nexus/analytics/starrocks/root-password).Trim()

$env:DFS_KAFKA_BOOTSTRAP     = '192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092'
$env:DFS_KAFKA_CA            = "$sec\kafka-ca.crt"
$env:DFS_KAFKA_CERT          = "$sec\kafka-client.crt"
$env:DFS_KAFKA_KEY           = "$sec\kafka-client.key"
$env:DFS_SR_URL              = 'https://192.168.10.91:8081'
$env:DFS_STARROCKS_CONNECTION = "Server=192.168.70.31;Port=9030;User ID=root;Password=$sr;SslMode=None;AllowPublicKeyRetrieval=true"
$env:GITHUB_PACKAGES_TOKEN   = (gh auth token)

Write-Host 'Loading the StarRocks DWH from the curated topics...' -ForegroundColor Cyan
dotnet run --project "$repo\src\DataFlowStudio.WarehouseSink" -c Release
