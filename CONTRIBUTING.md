# Contributing to DataFlow Studio

This is a personal portfolio project (sole author: Grigoris Zapantis). These conventions keep it
consistent with the rest of the NexusPlatform portfolio.

## Prerequisites

- **.NET 10 SDK** (pinned in [`global.json`](global.json)).
- A SQL Server for the migration gate: **LocalDB** locally, or Docker (Testcontainers) in CI.
- **Node.js** (optional, local) to activate the Conventional-Commits git hook: `npm install`.

## Build & test

```bash
dotnet restore DataFlowStudio.slnx
dotnet build   DataFlowStudio.slnx -c Release   # warnings are errors
dotnet format  DataFlowStudio.slnx --verify-no-changes
dotnet test    DataFlowStudio.slnx
```

To run the E1 migration gate against LocalDB instead of a container:

```bash
# PowerShell
$env:DFS_OLTP_TEST_CONNECTION = "Server=(localdb)\MSSQLLocalDB;Database=master;Integrated Security=true;TrustServerCertificate=true;Encrypt=false"
dotnet test tests/DataFlowStudio.Migrations.Tests
```

## Branching (MASTER-PLAN §7.2)

- `main` is protected. Work on `feat/<topic>`, `fix/<topic>`, or `chore/<topic>` branches.
- Semantic versioning per release; `v0.1.0` requires the §6 acceptance gate green.

## Commit messages

**Conventional Commits** — enforced by commitlint via the Husky `commit-msg` hook
(`npm install` activates it). Examples:

```
feat(ingestion): add CDC poll loop for dbo.Customers
fix(migrations): drop system-versioning before dropping temporal table
chore(deps): bump Testcontainers.MsSql
```

## Every PR

Must fill the four-skill-dimensions checklist in the PR template. Missing evidence blocks merge.
Update `CHANGELOG.md` under `[Unreleased]`.

## Architecture rules

Module boundaries are enforced by NetArchTest ([ADR-0001](docs/adr/ADR-0001-modular-monolith.md)).
Do not add a project reference between modules — communicate via `SharedKernel` integration events.
EF Core is banned on the AOT paths ([ADR-0002](docs/adr/ADR-0002-dapper-fluentmigrator-on-aot-paths.md)).
