#requires -version 7
<#
.SYNOPSIS
  Seed a representative order-flow dataset into the live OltpDb.
.DESCRIPTION
  Runs the DataFlowStudio.Seed tool against the SQL AG primary, inserting a coherent set of
  customers, products, categories, warehouses, addresses, inventory, orders, order lines,
  transactions, and shipments. Idempotent: a marker row short-circuits a re-run. Reads the SQL SA
  password from Vault.

  Prerequisites: the SQL AG tier powered on, OltpDb migrated (Week 1), and Vault unsealed.
#>
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent

$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$vault = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\vault.exe"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token

$sa = (& $vault kv get -field=content nexus/oltp/sqlserver/sa-password).Trim()
$env:DFS_SQL_CONN = "Server=192.168.70.16,1433;Database=OltpDb;User Id=sa;Password=$sa;Encrypt=True;TrustServerCertificate=True"
$env:GITHUB_PACKAGES_TOKEN = (gh auth token)

Write-Host 'Seeding OltpDb with the order-flow dataset...' -ForegroundColor Cyan
dotnet run --project "$repo\src\DataFlowStudio.Seed" -c Release
