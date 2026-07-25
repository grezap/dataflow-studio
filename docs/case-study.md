# Case study — DataFlow Studio

> A production-shaped change-data-capture pipeline, built as one deployable, tested to a hard gate,
> orchestrated, packaged, and observable across three planes. This is the story of the engineering
> choices behind it.

## The problem

Move a commerce dataset from an OLTP source of truth into analytics stores **without** coupling the
analytics path to the operational database, **without** losing governance over the event contracts, and
**with** enough observability to answer "is the pipeline healthy and how fresh is its view of the
source?" — the everyday questions of a data platform.

## The shape

```
OltpDb (SQL Server AG)  ──log-based CDC──►  Debezium raw topics  ──.NET curation──►  curated Avro (SR)
                                                                                          │
                                    StarRocks Kimball DWH  ◄──.NET warehouse sink────────┤
                                    (SCD2 dims + facts)                                   │
                                    ClickHouse analytics   ◄──native Kafka-engine─────────┘
                                    (pipeline telemetry)         (self-observation)
```

A **modular monolith**: one deployable, four isolated modules (Commerce, Ingestion, Warehouse,
Telemetry). The boundaries are not a convention — they are **executable**: NetArchTest fails the build if
a module references another module or the host, and a second rule enforces "no EF Core on the CDC paths".

## The engineering choices (and why)

| Decision | Why | ADR |
|---|---|---|
| Modular monolith, boundaries enforced by tests | One thing to build/run/demo, but it can split into services later without a rewrite | [0001](adr/ADR-0001-modular-monolith.md) |
| Dapper + FluentMigrator/DbUp, **no EF Core** on CDC + migration paths | Keeps those paths trim/AOT-friendly and the SQL explicit | [0002](adr/ADR-0002-dapper-fluentmigrator-on-aot-paths.md), [0007](adr/ADR-0007-data-driven-curation-catalog.md) |
| Debezium raw capture + a .NET **curation** worker | Log-based CDC never queries the OLTP tables; curation gives clean, versioned Avro contracts | [0004](adr/ADR-0004-cdc-transport-debezium-curation.md) |
| A **data-driven** curation catalog | Adding an entity is a list entry, not new code | [0007](adr/ADR-0007-data-driven-curation-catalog.md) |
| StarRocks SCD2 via PK-model `UPDATE` + batched `INSERT` (no `MERGE`) | StarRocks has no `MERGE`; batching avoids the version-explosion penalty | [0006](adr/ADR-0006-sink-load-strategy.md) |
| ClickHouse telemetry via **native Kafka-engine** ingestion | The pipeline observes itself without a bespoke ingestion service; epoch-ms timestamps stay locale-safe | [0008](adr/ADR-0008-clickhouse-native-telemetry-ingestion.md) |
| Three self-observation planes correlated by one id | One run = one OTel trace id = one ClickHouse `trace_id` = one Marquez `runId` | [0010](adr/ADR-0010-opentelemetry-otlp-export.md), [0011](adr/ADR-0011-openlineage-marquez-emission.md) |
| Shouldly over FluentAssertions | FA v8+ is proprietary; a portfolio should not rely on an undefined "commercial" boundary | [0009](adr/ADR-0009-shouldly-over-fluentassertions.md) |
| Coverage via DIP seams, not mocked infrastructure | The loaders/sinks/emitters test against `IStarRocksClient` / an injectable handler — real logic, no lab | [0012](adr/ADR-0012-test-coverage-strategy.md) |

## The results

- **Tested to the gate.** 113 tests (102 unit + 6 architecture + 2 Api boot-smoke + 3 container-gated
  migration tests). Logic coverage **93% line / 85% branch**, ≥80/80 on every logic assembly, enforced in
  CI ([ADR-0012](adr/ADR-0012-test-coverage-strategy.md)).
- **Governed.** 10 versioned curated Avro contracts through the Schema Registry; the OltpDb schema is
  reversible (FluentMigrator up→down→up, gated in CI).
- **Observable.** Every run lands as a Tempo trace, a set of ClickHouse `pipeline_events` (latency + CDC
  lag), and a Marquez lineage graph (2 jobs, 29 datasets) — all sharing one id.
- **Orchestrated + packaged.** One `.NET Aspire` AppHost brings up the whole topology; the same projects
  build into non-root container images with `docker-compose`, a Swarm `stack.yml`, and Kubernetes
  manifests. `nexus-cli deploy dataflow-studio` plans the end-to-end deploy.
- **Silver layer.** A strictly-typed **PySpark** job conforms the gold star into a `customer_360` mart
  ([ADR-0014](adr/ADR-0014-pyspark-silver-conformance.md)).

## What it demonstrates

The four portfolio dimensions, each with a concrete artefact: **.NET engineering** (enforced boundaries,
DIP seams, no-EF-on-AOT), **advanced SQL** (CDC + temporal tables, SCD2, aggregating MVs), **Python**
(the PySpark conformance job), and **DevOps** (three observation planes, container packaging, the deploy
verb). The full replay is in [`handbook.md`](handbook.md); the five-minute tour is [`demo.md`](demo.md).
