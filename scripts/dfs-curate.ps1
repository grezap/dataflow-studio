#requires -version 7
<#
.SYNOPSIS
  Run the DataFlow Studio curation worker in drain mode against the live lab.
.DESCRIPTION
  Consumes every raw Debezium CDC topic in the catalog, reshapes each change into curated Avro, and
  produces it to the dfs.<entity>.changed.v1 topics through the Schema Registry — stopping once the
  raw snapshot is fully consumed, then printing a per-entity count. Handles the lab plumbing: issues
  a short-lived Kafka mTLS client certificate and sets the environment the curator expects.

  Prerequisites: kafka-east + schema-registry + kafka-connect powered on, the Debezium 'oltp-cdc'
  connector running and capturing all order-flow tables, OltpDb seeded, and Vault unsealed. The Kafka
  consumer group 'dfs-curation*' and the 'dfs.*' topics must be granted ACLs (see the phase notes).
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

$env:DFS_KAFKA_BOOTSTRAP = '192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092'
$env:DFS_KAFKA_CA        = "$sec\kafka-ca.crt"
$env:DFS_KAFKA_CERT      = "$sec\kafka-client.crt"
$env:DFS_KAFKA_KEY       = "$sec\kafka-client.key"
$env:DFS_SR_URL          = 'https://192.168.10.91:8081'
$env:GITHUB_PACKAGES_TOKEN = (gh auth token)

Write-Host 'Draining raw CDC into curated Avro...' -ForegroundColor Cyan
dotnet run --project "$repo\src\DataFlowStudio.Curation" -c Release
