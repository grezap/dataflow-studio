# Changelog

All notable changes to DataFlow Studio are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — replay completeness (full-replay documentation standard)

- **`DataFlowStudio.Telemetry` console + `scripts/dfs-telemetry.ps1`** — reads the pipeline's own
  telemetry back out of ClickHouse (Kafka-engine objects, `pipeline_events`, `cdc_lag_seconds`, the
  p50/p95/p99 latency MV, `error_events`), and its `demo-errors` mode emits one error down **each**
  path — native Kafka→Kafka-engine and the .NET HTTPS control path — then waits for both to land.
  This replaces a throwaway harness used once during 3D: a proof only the author can run is a
  documentation gap, so it is now committed, runnable code.
- Handbook §1.8a rewritten around the script (the raw SQL is kept, folded into a details block).

### Changed — assertion library: FluentAssertions → Shouldly

- **Migrated the whole test suite to [Shouldly](https://github.com/shouldly/shouldly) (MIT)**
  (ADR-0009). FluentAssertions v8+ is proprietary — commercial use requires a paid Xceed licence, and
  Xceed's public FAQ does not define "commercial" vs "non-commercial"; v7 stays Apache-2.0 but is
  critical-fixes-only. Shouldly is MIT and independently maintained, closing the question permanently.
- Done now, at **82 assertions across 9 files**, rather than after the Week-4 80%-coverage push.
- An audit before deciding showed the usual objections to Shouldly do not apply here: **zero**
  `AssertionScope` uses, and the 3 `BeEquivalentTo` uses are collection comparisons (→
  `ShouldBe(…, ignoreOrder: true)`), not object-graph diffing.
- **Mutation-checked**: corrupting an expected value and a collection member made the suite fail,
  confirming the rewritten assertions still assert.
- Dependabot PR #7 (`FluentAssertions 6.12.2 → 8.10.0`) closed as superseded; MASTER-PLAN E12 updated.

### Added — Week 3 (Session 3D): ClickHouse telemetry sink (native Kafka-engine ingestion)

- **The pipeline observes itself** (ADR-0008) — the curation engine emits a stage event per curated
  record plus an end-to-end **CDC-lag** sample (`now − source-commit time`), and the warehouse sink
  emits one event per loader stage (`dim_*`, `fact_*`). Failures emit structured error events.
- **Native ingestion** — workers produce JSON to `dfs.telemetry.{pipeline_events,cdc_lag,error_events}`
  and **ClickHouse ingests it itself** through Kafka-engine source tables + materialized views
  (`Script0005`); no .NET consumer sits on that path. Timestamps cross as epoch-ms `event_ms` and the
  MVs convert with `fromUnixTimestamp64Milli`, avoiding datetime-string parsing on the broker path.
- **Telemetry seam in the SharedKernel** — `IPipelineTelemetrySink` + `PipelineStageEvent` /
  `CdcLagSample` / `PipelineError` (pure contracts) let the Ingestion and Warehouse engines emit
  without referencing the Telemetry module; `NullPipelineTelemetrySink` keeps unwired runs clean.
- **Dual, non-duplicating error paths** — errors flow natively like everything else; when the broker
  is unreachable (so the error is often *about* Kafka) `ClickHouseErrorSink` inserts straight over
  ClickHouse HTTPS.
- **New `DataFlowStudio.Clickhouse` library** — the private-CA TLS connection factory moved out of the
  migrations tool so the Telemetry module can share it, without leaking `ClickHouse.Client` into the
  AOT-friendly SharedKernel.
- **Topology-aware migrations** — `ClickHouseMigrationProfile.ExcludedScripts`; the single-node
  profile skips the Kafka-engine script (a CI container has no broker), so the E1 gate stays green.
- **ClickHouse as a Kafka client** — a dedicated `kafka-clickhouse-client` PKI role issues
  `CN=clickhouse-telemetry`; the data nodes carry the PEMs plus a `<kafka>` config block, and the
  principal holds `READ`/`DESCRIBE` on topic-prefix `dfs.telemetry` and `READ` on group-prefix
  `dfs-clickhouse`. TLS never appears in DDL.
- **E16 scaffolding** — an OpenTelemetry counter per emit; OTLP export wires up only when
  `DFS_OTLP_ENDPOINT` is set (the observability tier lands in 3E).
- **Live-proven on the lab** — one curate + one sink produced **88 `pipeline_events`** across both
  pipelines, **77 `cdc_lag_seconds`** samples (min ~55 s for freshly-issued source changes; historical
  records correctly report their true age), and `error_events` via **both** paths. The
  `pipeline_latency_by_hour` `AggregatingMergeTree` MV returns real p50/p95/p99 per stage, and
  re-running the migration applies 0 scripts.
- **Retired a 3C workaround** — the DWH sink now has its own `dfs-warehouse-sink` group ACL instead of
  borrowing the `dfs-curation` prefix.

### Added — Week 3 (Session 3C): StarRocks DWH sink (SCD2 dimensions + facts)

- **Warehouse sink** (ADR-0006) — the Warehouse module consumes the curated Avro topics and loads the
  Kimball star over the MySQL wire: **SCD2** `dim_customer`/`dim_product` (close-current + insert new
  version by surrogate key; unchanged → no-op), SCD1 `dim_warehouse`/`dim_carrier`, generated
  `dim_date`, and the four facts (`fact_order`/`fact_order_line`/`fact_transaction`/
  `fact_inventory_snap`) with on-demand range-partition creation. Loads in dependency order; inserts
  are batched (StarRocks penalises single-row inserts); `line_total_usd` is recomputed.
- **Runnable sink** — `DataFlowStudio.WarehouseSink` (drain, `scripts/dfs-warehouse-sink.ps1`) +
  the hosted `WarehouseSinkWorker` (timer), sharing one `WarehouseSinkEngine`.
- **ADR-0006** (sink load strategy: StarRocks .NET worker now; ClickHouse-native in 3D) + Sql-literal
  unit tests. At-least-once curated topics are deduped by natural key; every load path is idempotent.
- **Live-proven on the lab** — loaded **59 curated records** into the star: dim_customer(8)/dim_product(6)
  current, dim_warehouse(3), dim_carrier(2), dim_date(5), fact_order(4)/fact_order_line(6)/
  fact_transaction(4)/fact_inventory_snap(18); a star join resolves SCD2 surrogate keys; a re-run adds
  no versions/rows (idempotent). One fix surfaced live: `product-inventory` is keyed by product alone,
  so the sink dedups it by its full (product, warehouse) grain.

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
