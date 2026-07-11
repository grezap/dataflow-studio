# ADR-0004 — CDC transport: Debezium raw capture + a .NET curation worker

- **Status:** accepted
- **Date:** 2026-07-11
- **Deciders:** Grigoris Zapantis

## Context

Changes in `OltpDb` (SQL Server AG) must reach Kafka. Three shapes were on the table:

1. **Custom .NET worker only** — poll SQL Server CDC via Dapper and produce to Kafka directly.
   Strongest .NET showcase, but re-implements robust CDC capture (offset/LSN tracking, snapshots,
   schema history) that a mature tool already solves.
2. **Debezium only** — the pre-loaded Debezium SQL Server connector (Kafka Connect) streams CDC to
   Kafka; the .NET side is just a consumer. Industry-standard, least custom code, weakest .NET story.
3. **Both** — Debezium captures *raw* CDC; a .NET worker *curates* it into clean domain events.

## Decision

Adopt **option 3 — the realistic enterprise pattern**:

- **Debezium** (`io.debezium.connector.sqlserver.SqlServerConnector` on Kafka Connect) captures
  `OltpDb` CDC into **raw** Kafka topics (`oltp.OltpDb.dbo.*`) as JSON. It owns the hard parts:
  transaction-log reading, initial snapshot, schema history, LSN offsets. It talks to Kafka over the
  worker's mTLS producer; its schema-history producer/consumer are separately configured for mTLS.
- **A dataflow-studio curation worker** (the Ingestion module; the `dfs trace` tool embeds the same
  logic) consumes the raw topics, **reshapes each change into a clean domain Avro event**
  (`CustomerChanged`, …), and re-produces to **curated** topics (`dfs.*.v1`) via `Nexus.Kafka` +
  `Nexus.Avro`, presenting a Vault-issued mTLS client cert (`Nexus.Vault`). This is the .NET
  engineering + domain-modelling showcase, and it decouples every downstream consumer from Debezium's
  raw envelope.

**Debezium emits JSON, not Avro** (the Connect worker's default `JsonConverter`); Avro lives on the
**curated** topics, produced by the .NET worker (see [ADR-0003](ADR-0003-avro-schema-registry.md)).
The Confluent Avro converter is present on the nodes but not on the Connect plugin path, and
JSON-raw + Avro-curated is cleaner anyway (native Debezium output; typed contract downstream).

## Consequences

- **Positive:** best of both — Debezium's battle-tested capture + a typed, curated domain contract in
  .NET. Downstream binds to the stable `dfs.*.v1` Avro schema, insulated from source-schema churn.
- **Negative:** two hops (raw + curated) and more topics than a single-transport design; two things
  to operate (the connector + the worker). Accepted for the decoupling + showcase value.
- **Lab specifics (operational):** the KRaft brokers have **auto-create off**, so both Debezium
  (Connect KIP-158 `topic.creation.default.*`) and the worker (AdminClient `CreateTopics`) create
  their topics explicitly; and the brokers enforce **ACLs** (only broker principals are super-users),
  so the dataflow-studio client principal gets **least-privilege ACLs** (read `oltp.*`, read/write
  `dfs.*`, read group `dfs-trace*`). Both are documented in `docs/demos/watch-the-pipeline.md`.
