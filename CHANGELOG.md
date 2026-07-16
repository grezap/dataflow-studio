# Changelog

All notable changes to DataFlow Studio are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Week 3 (Session 3B): source replay — curated events for all order-flow entities

- **Data-driven curation catalog** (ADR-0007) — one `EntityCurationSpec` per order-flow entity
  (customers, product categories, products, warehouses, customer addresses, orders, order lines,
  transactions, shipments, product inventory) mapping a Debezium raw topic → `dfs.<entity>.changed.v1`
  + a generated curated Avro schema. Adding an entity is a list entry, not new worker code.
- **Curation engine + worker** — the Ingestion module is now the real (non-AOT) CDC curation worker:
  consume raw Debezium JSON → project (`CuratedRecordProjector`, pure + unit-tested) → produce curated
  Avro through the Schema Registry. Runs continuously (`CurationWorker`) or in drain mode (the
  runnable `DataFlowStudio.Curation` console, `scripts/dfs-curate.ps1`). Decimals carried as strings;
  timestamps as epoch-millis longs.
- **Seed tool** (`DataFlowStudio.Seed`, `scripts/dfs-seed.ps1`) — an idempotent, representative
  OltpDb order-flow dataset (4 customers, 6 products, 3 warehouses, 4 orders + lines/transactions/
  shipments/inventory) so the curated topics have real content.
- **AsyncAPI 0.3.0** — all ten curated channels + the shared curated-change envelope.
- **AOT resolution (ADR-0007)** — Debezium+curation (ADR-0004) + reflection-based Avro serdes
  (ADR-0003) leave no Native-AOT .NET worker; the Ingestion module drops its AOT badge but keeps the
  no-EF-Core invariant (still enforced by the architecture tests).
- **Live-proven on the lab** — seeded OltpDb → Debezium captured all 10 tables (`time.precision.mode
  =connect`) → the curator produced **59 curated records across all 10 topics**, all 10 Schema
  Registry subjects registered. Two refinements surfaced live: curated Avro fields now carry
  **defaults** so schema evolution is BACKWARD-compatible (adding `preferredLocale` made customers a
  clean v2); and the computed `LineTotalUsd` column (stored NULL by SQL Server CDC) is **not carried**
  — the DWH loader recomputes it. At-least-once delivery means curated topics can carry duplicates
  (keyed by natural key), so the sink loaders (3C) must be idempotent/upsert.

### Added — Week 3 (Session 3A): sink schema migrations (DbUp)

- **`DataFlowStudio.Migrations.Starrocks`** — DbUp (`dbup-mysql` over MySqlConnector) reproducing the
  `dwh` Kimball star (5 dimensions, 4 facts, `bridge_customer_seg`) + an `analytics` serving view,
  with a StarRocks-compatible journal (`StarRocksTableJournal`) and a `$replicationNum$` variable
  (3 for the lab, 1 for a single-backend container).
- **`DataFlowStudio.Migrations.Clickhouse`** — a purpose-built, DbUp-pattern runner over
  `ClickHouse.Client` reproducing the `analytics` telemetry schema (`pipeline_events` local +
  Distributed, the `pipeline_latency_by_hour` AggregatingMergeTree MV, `cdc_lag_seconds`,
  `error_events`), with lab-vs-single-node profiles (`Replicated*MergeTree` + `ON CLUSTER` vs plain).
- **Idempotency gates (E1, forward-only)** — `apply → re-apply` on throwaway `starrocks/allin1` +
  `clickhouse-server` containers; both green. The gates caught four defects in the authored DDL
  (PK distribution key, colocation bucket count, key-column order, `nexus_ch` → `nexus_analytics`),
  fixed in the scripts and mirrored back into `schemas/dataflow-studio/README.md`.
- **ADR-0005** — DbUp migrations for the StarRocks + ClickHouse sinks (the split + the corrections).

### Added — Week 2: CDC → Kafka, live on the lab

- **`OltpDb` on the SQL Server AG** — the schema (11 tables) applied by the FluentMigrator runner
  against the live Always-On primary, with **SQL Server CDC enabled** on every business table.
- **Debezium** SQL Server connector (`oltp-cdc`) streaming raw CDC → Kafka JSON topics over mTLS
  (schema-history producer mTLS; Connect KIP-158 topic creation).
- **`DataFlowStudio.Trace`** — a runnable 5-face demo (`scripts/dfs-trace.ps1`) that follows one
  record OLTP → CDC → Debezium raw → **curated Avro** (Schema Registry) → sink, consuming
  `Nexus.Kafka` / `Nexus.Avro` / `Nexus.Primitives` from GitHub Packages over mTLS (least-privilege
  Kafka ACLs for the app principal).
- **E8 extraction** — `Result`/`Error`/`AuditColumns` now come from the published `Nexus.Primitives`.
- **Docs** — ADR-0003 (Avro + Schema Registry, AOT relaxation), ADR-0004 (CDC transport: Debezium +
  curation), AsyncAPI for the raw + curated topics, and `docs/demos/watch-the-pipeline.md` (by-hand
  observation with SSMS + Kafka console tools).

### Added

- **Solution scaffold** — modular-monolith solution (`.slnx`): `Api` composition root, `SharedKernel`,
  four module assemblies (`Commerce`, `Ingestion`, `Warehouse`, `Telemetry`), and the
  `Migrations.Oltp` tool. House conventions mirrored from `nexus-cli` (central package management,
  `net10.0`, warnings-as-errors, AOT analyzers).
- **OltpDb migrations** — FluentMigrator migrations for all 11 tables from `schemas/dataflow-studio`
  (system-versioned temporal `Customers`/`Products`, `audit.ChangeLog`, persisted computed
  `LineTotalUsd`, `ROWVERSION`, standard audit columns), each with a reversible `Down()`.
- **E1 acceptance gate** — `up → down → up` migration test on a fresh SQL Server (Testcontainers in
  CI, LocalDB locally).
- **Architecture tests** — NetArchTest rules enforcing module isolation and "no EF Core on AOT paths"
  (E4).
- **Repo hygiene** — README (14-section), CONTRIBUTING, ADR-0001/0002, `docs/sql-showcase.md`,
  OpenAPI + AsyncAPI stubs, devcontainer, GitHub Actions CI, Dependabot, PR template, Conventional
  Commits tooling.
- **Documentation as a rule** — `docs/architecture.md` with rendered Mermaid diagrams (system
  context, module dependency graph, a row's end-to-end journey, cross-system blast-radius). XML
  `<summary>` docs on every public type/member plus analytical inline comments, **enforced in build**
  via `GenerateDocumentationFile` (missing public docs = CS1591 error under warnings-as-errors).

[Unreleased]: https://github.com/grezap/dataflow-studio/commits/main
