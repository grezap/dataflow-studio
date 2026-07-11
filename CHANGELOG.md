# Changelog

All notable changes to DataFlow Studio are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
