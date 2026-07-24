# Architecture Decision Records

ADR lifecycle (MASTER-PLAN §7.3): `planned` → `proposed` → `accepted` | `deprecated` | `superseded`.

| ADR | Title | Status | Date |
|---|---|---|---|
| [ADR-0001](ADR-0001-modular-monolith.md) | Modular Monolith with enforced module boundaries | accepted | 2026-07-11 |
| [ADR-0002](ADR-0002-dapper-fluentmigrator-on-aot-paths.md) | Dapper + FluentMigrator on AOT paths; no EF Core on the Kafka worker | accepted | 2026-07-11 |
| [ADR-0003](ADR-0003-avro-schema-registry.md) | Avro + Schema Registry for curated events (and the AOT relaxation for the Avro serdes) | accepted | 2026-07-11 |
| [ADR-0004](ADR-0004-cdc-transport-debezium-curation.md) | CDC transport: Debezium raw capture + a .NET curation worker | accepted | 2026-07-11 |
| [ADR-0005](ADR-0005-dbup-sink-migrations.md) | DbUp migrations for the StarRocks + ClickHouse sinks | accepted | 2026-07-12 |
| [ADR-0006](ADR-0006-sink-load-strategy.md) | Sink load strategy (StarRocks .NET worker + ClickHouse native) | accepted | 2026-07-17 |
| [ADR-0007](ADR-0007-data-driven-curation-catalog.md) | A data-driven curation catalog (Ingestion module's non-AOT stance) | accepted | 2026-07-17 |
| [ADR-0008](ADR-0008-clickhouse-native-telemetry-ingestion.md) | ClickHouse native telemetry ingestion + the pipeline-telemetry seam | accepted | 2026-07-19 |
| [ADR-0009](ADR-0009-shouldly-over-fluentassertions.md) | Shouldly instead of FluentAssertions (assertion-library licence risk) | accepted | 2026-07-19 |
| [ADR-0010](ADR-0010-opentelemetry-otlp-export.md) | OpenTelemetry OTLP export to the lab observability tier (E16) | accepted | 2026-07-24 |

> Target for v0.1.0: ≥5 ADRs (MASTER-PLAN §6) — **met** (10 ADRs). ADR-0008 completes the
> ClickHouse-native half of ADR-0006 (Week-3 Session 3D); ADR-0009 supersedes the FluentAssertions
> choice recorded in MASTER-PLAN E12; ADR-0010 wires the OTel export the telemetry seam (ADR-0008) was
> built for, once the observability tier came online (Week-3 Session 3E.2).
