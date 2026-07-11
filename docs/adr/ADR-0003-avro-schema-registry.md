# ADR-0003 — Avro + Schema Registry for curated events (and the AOT relaxation)

- **Status:** accepted
- **Date:** 2026-07-11
- **Deciders:** Grigoris Zapantis

## Context

The pipeline's curated Kafka events must be **self-describing, schema-validated, and evolvable** —
downstream consumers (the StarRocks DWH, the ClickHouse telemetry, and future services) need a
stable typed contract, not a hand-parsed JSON blob. The MASTER-PLAN grid calls for "Kafka (Avro via
Schema Registry)".

This collides with [ADR-0002](ADR-0002-dapper-fluentmigrator-on-aot-paths.md), which put the Kafka
worker on a **Native-AOT** path: the .NET Avro stack (`Confluent.SchemaRegistry.Serdes.Avro` +
`Apache.Avro`, over `Confluent.Kafka`/librdkafka) uses reflection and runtime schema handling that
is **not cleanly AOT/trim-safe**.

## Decision

- **Curated events are Avro, registered in the Confluent Schema Registry.** The curated topics
  (`dfs.*.v1`) carry Avro records; the serializer auto-registers the schema under the topic-value
  subject on first publish, so every message on the wire carries a schema id and is validated +
  versioned by the registry. The wire contract is documented in `docs/api/asyncapi.yaml` (E14).
- **The Avro/Schema-Registry path relaxes the strict Native-AOT-publish requirement of ADR-0002.**
  The Kafka-facing code (curation worker, `dfs trace` tool) stays **Dapper-only / no EF Core** and
  trim-analyzer-aware, but is **not AOT-published**, because the Avro serdes + librdkafka wrapper are
  not AOT-compatible. This is a deliberate, scoped trade-off: correctness + schema governance win
  over an AOT binary for the Kafka worker specifically.
- **AOT is retained where it is free.** The FluentMigrator migration tool (no Avro, no Kafka) and
  `Nexus.Primitives` remain AOT-clean. The "no EF Core on the data paths" rule (ADR-0002) is
  unchanged and still enforced by the architecture tests.
- **Schema Registry TLS:** the lab registry is server-TLS on `:8081`. Clients trust the Nexus CA (or,
  in the lab demo, skip verification via `SchemaRegistryOptions.EnableCertificateVerification`).

## Consequences

- **Positive:** schema evolution + compatibility are enforced by the registry, not by hope; the
  curated contract is decoupled from Debezium's raw envelope; consumers bind to a versioned schema.
- **Negative:** the Kafka worker ships as a normal (framework-dependent or self-contained) binary,
  not Native AOT — a larger footprint and slower cold start than the CLI. Accepted: the worker is a
  long-running service where startup time is irrelevant, and the schema-registry integration is worth
  more than the AOT size.
- **Follow-up:** the reusable Avro/SR helpers live in `Nexus.Avro` (nexus-shared); a second consumer
  reuses them without re-solving the AOT/serdes questions.
