# DataFlow Studio

[![ci](https://github.com/grezap/dataflow-studio/actions/workflows/ci.yml/badge.svg)](https://github.com/grezap/dataflow-studio/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Portfolio Project #1 — NexusPlatform.** A modular-monolith data pipeline that moves a commerce
> dataset from an OLTP source of truth into a Kimball warehouse and a real-time telemetry store:
> **SQL Server AG (CDC) → Kafka (Avro / Schema Registry) → StarRocks DWH + ClickHouse analytics.**

---

## 1. Overview

DataFlow Studio is the first application project built **on** the NexusPlatform lab rather than
provisioning it. It demonstrates a production-shaped change-data-capture pipeline end to end, with
enforced architecture boundaries, reversible database migrations, and first-class observability.

## 2. What it demonstrates — the four skill dimensions

| Dimension | In this project |
|---|---|
| **.NET engineering + architecture** | Modular monolith with module boundaries enforced by NetArchTest ([ADR-0001](docs/adr/ADR-0001-modular-monolith.md)); Native-AOT Kafka worker ([ADR-0002](docs/adr/ADR-0002-dapper-fluentmigrator-on-aot-paths.md)). |
| **Advanced SQL + analytics** | Temporal tables, persisted computed columns, `ROWVERSION` concurrency, Kimball star schema, ClickHouse aggregating MVs. See [`docs/sql-showcase.md`](docs/sql-showcase.md). |
| **Python** | *(Week 3+)* PySpark / analytics tooling where applicable per the grid. |
| **DevOps literacy** | Operated via `nexus-cli`; deploy + migration gate automated; runbook with panic button. |

## 3. Architecture

A modular monolith — one deployable, four isolated module assemblies, a shared kernel, and an API
composition root:

```
DataFlowStudio.Api  (composition root — the only project that references every module)
├── Modules/Commerce    OLTP write-side over OltpDb (source of truth)
├── Modules/Ingestion   CDC → Kafka (Avro) worker — Native-AOT path (Dapper, no EF Core)
├── Modules/Warehouse   StarRocks Kimball DWH loaders            (Week 3)
├── Modules/Telemetry   ClickHouse pipeline telemetry writers     (Week 3)
└── SharedKernel        Result pattern · audit columns · IModule · IntegrationEvent
DataFlowStudio.Migrations.Oltp   FluentMigrator migrations for OltpDb (up/down)
```

Modules never reference one another; cross-module communication is via `SharedKernel` integration
events. The rules are executable — the architecture tests fail the build if a boundary erodes.

## 4. Data flow

```
OltpDb (SQL Server AG)  ──CDC──►  Kafka topics (Avro, Schema Registry)  ──►  StarRocks dwh + analytics
        │  11 tables, temporal + audit                                  └──►  ClickHouse analytics (pipeline telemetry, CDC lag, errors)
```

## 5. Tech stack

.NET 10 / C# · ASP.NET Core minimal API · Dapper · FluentMigrator (OltpDb) + DbUp (StarRocks/ClickHouse) ·
Apache Kafka + Avro + Schema Registry · StarRocks · ClickHouse · OpenTelemetry → Grafana LGTM ·
OpenLineage → Marquez · xUnit + Testcontainers + NetArchTest · Native AOT (Kafka worker).

## 6. Getting started

Prerequisites: **.NET 10 SDK** (see [`global.json`](global.json)) and, for the migration gate, a
SQL Server (LocalDB locally or Docker for Testcontainers in CI).

```bash
dotnet restore DataFlowStudio.slnx
dotnet build   DataFlowStudio.slnx -c Release
dotnet test    DataFlowStudio.slnx
```

Run the API host:

```bash
dotnet run --project src/DataFlowStudio.Api
# GET /health   GET /modules   GET /openapi/v1.json (Development)
```

## 7. Project structure

```
src/    DataFlowStudio.Api · SharedKernel · Migrations.Oltp · Modules/{Commerce,Ingestion,Warehouse,Telemetry}
tests/  Architecture.Tests (NetArchTest) · Migrations.Tests (E1 gate) · UnitTests
docs/   adr/ · sql-showcase.md · api/{openapi,asyncapi}.yaml
```

## 8. Database & migrations

`OltpDb` is owned by FluentMigrator (`DataFlowStudio.Migrations.Oltp`). Every migration is
reversible. The **E1 acceptance gate** runs `up → down → up` on a fresh SQL Server in CI (and
against LocalDB locally):

```bash
# apply / roll back manually
dotnet run --project src/DataFlowStudio.Migrations.Oltp -- up   --connection "<conn>"
dotnet run --project src/DataFlowStudio.Migrations.Oltp -- down --connection "<conn>"
```

## 9. Configuration

Connection strings and secrets are injected at deploy time by the Vault Agent
(`nexus/dataflow-studio/*`). No credentials are committed; `appsettings.json` carries placeholders.

## 10. Testing

xUnit + FluentAssertions (unit) · NetArchTest (architecture) · Testcontainers (integration — real
SQL Server; real Kafka/StarRocks/ClickHouse in later weeks). Coverage gate: 80% application layer
(E12) — enforced from Week 4.

## 11. Observability

OpenTelemetry traces/metrics/logs flow to the Grafana LGTM stack (Phase 0.I); data lineage is
emitted via OpenLineage to Marquez (E16). Wired in Week 3.

## 12. Operations

Deployed and operated through `nexus-cli deploy dataflow-studio`. The runbook (Week 4) includes a
**panic button** to return to last-known-good.

## 13. Roadmap

| Week | Slice | Status |
|---|---|---|
| 1 | Solution scaffold · OltpDb migrations · E1 gate · repo hygiene | ✅ in progress |
| 2 | CDC → Kafka (Avro) · Schema Registry · AsyncAPI · first `nexus-shared` consumption · OpenLineage | ⏳ |
| 3 | StarRocks DWH + ClickHouse telemetry (DbUp) · OTel + Marquez | ⏳ |
| 4 | Tests to 80% · Aspire AppHost · Docker/Swarm/K8s · demo + recording · **v0.1.0** | ⏳ |

## 14. License

[MIT](LICENSE) © 2026 Grigoris Zapantis · Part of the [NexusPlatform](https://github.com/grezap) portfolio.
