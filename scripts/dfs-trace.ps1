#requires -version 7
<#
.SYNOPSIS
  Run the DataFlow Studio 5-face trace against the live NexusPlatform lab.
.DESCRIPTION
  Follows one customer record across all five faces of the pipeline
  (OLTP -> CDC -> Debezium/Kafka -> curated Avro -> sink), printing what it
  looks like at each hop. Handles the lab plumbing for you: reads the SQL SA
  password from Vault, issues a short-lived Kafka mTLS client certificate, and
  sets the environment the tool expects.

  Prerequisites: the SQL AG + kafka-east + schema-registry + kafka-connect tiers
  powered on, the Debezium 'oltp-cdc' connector running, and Vault unsealed.
#>
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent

# --- Vault (root token from the operator's init file; lab-local) ---
$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$vault = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\vault.exe"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token

# --- SQL SA password (OltpDb source) ---
$sa = (& $vault kv get -field=content nexus/oltp/sqlserver/sa-password).Trim()

# --- Issue a fresh 24h Kafka mTLS client cert into a git-ignored .secrets dir ---
$sec = Join-Path $repo '.secrets'
New-Item -ItemType Directory -Force -Path $sec | Out-Null
$issued = (& $vault write -format=json pki_int/issue/kafka-broker common_name=localhost ttl=24h) | ConvertFrom-Json
$issued.data.certificate | Set-Content "$sec\kafka-client.crt" -NoNewline
$issued.data.private_key  | Set-Content "$sec\kafka-client.key" -NoNewline
Copy-Item "$HOME\.nexus\vault-ca-bundle.crt" "$sec\kafka-ca.crt" -Force

# --- Environment the trace tool reads ---
$env:DFS_SQL_CONN        = "Server=192.168.70.16,1433;Database=OltpDb;User Id=sa;Password=$sa;Encrypt=True;TrustServerCertificate=True"
$env:DFS_KAFKA_BOOTSTRAP = '192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092'
$env:DFS_KAFKA_CA        = "$sec\kafka-ca.crt"
$env:DFS_KAFKA_CERT      = "$sec\kafka-client.crt"
$env:DFS_KAFKA_KEY       = "$sec\kafka-client.key"
$env:DFS_SR_URL          = 'https://192.168.10.91:8081'

# nexus-shared packages come from GitHub Packages (needs a read:packages token).
$env:GITHUB_PACKAGES_TOKEN = (gh auth token)

Write-Host "Running dfs trace against the live lab..." -ForegroundColor Cyan
dotnet run --project "$repo\src\DataFlowStudio.Trace" -c Debug
