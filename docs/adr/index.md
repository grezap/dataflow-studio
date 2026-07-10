# Architecture Decision Records

ADR lifecycle (MASTER-PLAN §7.3): `planned` → `proposed` → `accepted` | `deprecated` | `superseded`.

| ADR | Title | Status | Date |
|---|---|---|---|
| [ADR-0001](ADR-0001-modular-monolith.md) | Modular Monolith with enforced module boundaries | accepted | 2026-07-11 |
| [ADR-0002](ADR-0002-dapper-fluentmigrator-on-aot-paths.md) | Dapper + FluentMigrator on AOT paths; no EF Core on the Kafka worker | accepted | 2026-07-11 |

> Target for v0.1.0: ≥5 ADRs (MASTER-PLAN §6). ADR-0003 (Avro + Schema Registry contract strategy),
> ADR-0004 (CDC transport: SQL Server CDC + Debezium), and ADR-0005 (StarRocks/ClickHouse DbUp
> migration split) land with the Week-2/Week-3 slices.
