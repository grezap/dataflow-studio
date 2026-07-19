# ADR-0008 — ClickHouse native telemetry ingestion + the pipeline-telemetry seam

- **Status:** accepted
- **Date:** 2026-07-19
- **Deciders:** Grigoris Zapantis

## Context

ADR-0006 (D1) split the sink load strategy by store: a .NET worker owns the StarRocks DWH, and the
ClickHouse telemetry tables land **natively**. Session 3A migrated the ClickHouse `analytics` schema,
but nothing ever *emitted* or *ingested* telemetry — the Telemetry module was a skeleton.

Making the pipeline observe itself raised four design questions:

1. What wire format carries telemetry to ClickHouse, and how do timestamps survive the trip?
2. How do the Ingestion and Warehouse engines emit telemetry without referencing the Telemetry
   module (modules must stay isolated — ADR-0001, arch-test-enforced)?
3. `TlsClickHouseConnectionFactory` lived in `DataFlowStudio.Migrations.Clickhouse`; a module must not
   reference a migrations tool.
4. ClickHouse becomes a *Kafka client*, so it needs an identity, mTLS material, and ACLs.

## Decision

### 1. Native ingestion over Kafka, JSON / `JSONEachRow`
Workers produce one JSON line per record to `dfs.telemetry.pipeline_events`, `dfs.telemetry.cdc_lag`,
and `dfs.telemetry.error_events`. ClickHouse **Kafka-engine source tables + materialized views**
(`Script0005`) consume them into `pipeline_events_local` / `cdc_lag_seconds` / `error_events`. JSON
keeps ClickHouse off the Schema Registry (the curated *domain* events stay Avro — ADR-0003).

**Timestamps cross as epoch milliseconds** (`event_ms Int64`), and each MV converts with
`fromUnixTimestamp64Milli(event_ms) AS event_time`. This deliberately avoids datetime-*string*
parsing on the broker path, where `date_time_input_format` is a server/format setting rather than a
per-table one — the integer is unambiguous and needs no tuning.

### 2. The telemetry seam lives in the SharedKernel
`IPipelineTelemetrySink` + the `PipelineStageEvent` / `CdcLagSample` / `PipelineError` records live in
`DataFlowStudio.SharedKernel.Telemetry` — pure contracts, no Kafka or ClickHouse types. The Ingestion
and Warehouse engines depend only on that abstraction; the Telemetry module supplies the concrete
sink through DI. `NullPipelineTelemetrySink` is the default, so an unwired run executes cleanly and
instrumentation never disrupts the pipeline. Module isolation is preserved.

Record calls are cheap fire-and-forget enqueues (librdkafka's internal queue); `FlushAsync` completes
delivery at the end of a run.

### 3. A dedicated shared library for the ClickHouse TLS factory
`TlsClickHouseConnectionFactory` moved to a new **`DataFlowStudio.Clickhouse`** project, referenced by
both the migrations tool and the Telemetry module. It was deliberately *not* folded into the
SharedKernel: that assembly is `IsAotCompatible` pure contracts, and putting `ClickHouse.Client` there
would leak the dependency transitively into every module and the Api.

### 4. Errors take two non-duplicating paths
Errors flow **natively** like the other two streams. The direct-HTTPS inserter
(`ClickHouseErrorSink`, over the shared factory) is the **control/fallback** path: when the broker is
unreachable the worker cannot produce — and the error is often *about* Kafka — so it inserts straight
into `analytics.error_events`. One error takes one path, so no row is written twice.

### 5. ClickHouse authenticates as its own Kafka principal
A dedicated PKI role `pki_int/roles/kafka-clickhouse-client` issues `CN=clickhouse-telemetry`
(client-auth only). The existing shared `kafka-broker` role was **not** modified — it has
`allow_any_name=false` and a partial `vault write` would have reset its other fields. The nodes get
the PEMs plus a `<kafka>` block in `/etc/clickhouse-server/config.d/` (security_protocol `ssl` + CA +
client cert/key); the principal is granted `READ`/`DESCRIBE` on topic-prefix `dfs.telemetry` and
`READ` on group-prefix `dfs-clickhouse`. TLS material never appears in DDL.

### 6. The Kafka-engine script is lab-only
`ClickHouseMigrationProfile` gained `ExcludedScripts`; the `SingleNode` profile excludes
`Script0005_kafka_ingestion.sql` because a CI container has no broker. The E1 idempotency gate still
creates and asserts the rest of the schema, and the lab applies all five scripts.

### 7. E16 scaffolding only
The sink records an OpenTelemetry counter per emit and the module wires
`AddNexusObservability` **only when `DFS_OTLP_ENDPOINT` is set**. With the observability tier off
(until 3E) the `ActivitySource`/`Meter` have no listeners and cost nothing.

## Consequences

- **Positive:** the telemetry firehose never touches the .NET heap — ClickHouse pulls it directly.
  One shared TLS factory, one telemetry contract, and engines that stay ignorant of the sink. The
  latency MV gets real per-record samples, so `quantilesMerge` p50/p95/p99 are meaningful.
- **Negative:** ClickHouse is now a Kafka client, which is real lab surface — certs to rotate, ACLs to
  keep, and a `<kafka>` config block per data node. Kafka-engine ingestion is at-least-once and the
  telemetry tables are append-only, so duplicates are possible; that is acceptable for telemetry and
  is why no de-duplication is attempted.
- **Neutral:** with one partition per telemetry topic, exactly one ClickHouse node in each consumer
  group holds the assignment; the rows it writes replicate within its shard and are read back through
  the `Distributed` table (or `cluster(...)` for the MV, which has no distributed wrapper).
