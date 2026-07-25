# ADR-0012 — Test-coverage strategy for the E12 gate (80% on logic, seams over live IO)

- **Status:** accepted
- **Date:** 2026-07-25
- **Deciders:** Grigoris Zapantis

## Context

MASTER-PLAN **E12** and the Week-4 exit gate require **≥80% line and branch coverage**. The pipeline is
built from two kinds of code:

1. **Decision logic** — the curation projector (Debezium → curated Avro), the SCD2 dimension + fact
   loaders, the telemetry dual-path (native Kafka + HTTPS fallback), the OpenLineage event shaping, and
   the host-config option factories. This is pure, deterministic, and belongs under exhaustive unit test.
2. **Infrastructure-IO boundaries + composition roots** — the Kafka consume/produce-admin loops, the
   StarRocks MySQL-wire client, the ClickHouse private-CA connection factory, the DI modules, the
   `BackgroundService` host loops, and the console `Program` entry points. These only do meaningful work
   against **live** Kafka / StarRocks / ClickHouse / an OTLP collector, so a unit test cannot exercise
   them without reimplementing the broker/DB — the container gates and the live replay do that instead.

Measuring both kinds together with the default collector also counted **source-generated code** — the
`System.Text.Json` serializer contexts and the `LoggerMessage` partials (`*.g.cs`) — whose synthetic
branches a test can never reach. Left in, they dragged the branch number ~15 points below the real logic
coverage and made the gate un-meetable for reasons unrelated to test quality.

## Decision

**Gate on ≥80% line + branch over the hand-written *logic* assemblies. Make logic seam-testable rather
than mocking live infrastructure, and exclude generated code + the IO/composition boundaries explicitly.**

Three mechanisms:

### 1. Seams so the logic is testable without live infrastructure

Small dependency-inversion seams were introduced (all genuine design improvements, not test-only hooks):

| Seam | Lets us unit-test |
|---|---|
| `IStarRocksClient` (impl `StarRocksClient`) | `DimensionLoader` SCD2 (unchanged→no-op, changed→close+insert), `FactLoader` truncate-reload + `line_total` recompute, and `WarehouseSinkEngine.LoadStarAsync` orchestration — against a recording fake. |
| `IErrorFallbackSink` (impl `ClickHouseErrorSink`) + an internal `IProducer` ctor on `KafkaTelemetrySink` | The telemetry dual path: native produce, delivery-report fallback, and broker-unreachable fallback. |
| An internal `HttpMessageHandler` ctor on `MarquezLineageEmitter` | The OpenLineage START/COMPLETE/FAIL event shape, runId normalization, and best-effort swallow — against a stub handler. |
| `CurationEngine.TryCurateAsync` made `internal` | The per-record parse → project → produce → telemetry path against a recording producer. |

`WarehouseSinkEngine.RunAsync` was split into a Kafka-consume half and a pure `LoadStarAsync(byTopic,
IStarRocksClient, …)` half so the DWH load is testable independent of the broker. Internals are exposed
to the test assembly via `InternalsVisibleTo`.

### 2. Exclude generated code from the measurement

`coverlet.runsettings` excludes `**/*.g.cs` (the source-generated JSON contexts + `LoggerMessage`
partials) and trivial auto-property accessors (`SkipAutoProps`). The gate then reflects branches a test
can actually exercise. CI runs the unit suite with `--settings coverlet.runsettings` and fails below 80%.

### 3. Mark the IO/composition boundaries `[ExcludeFromCodeCoverage]`

With a one-line justification each. These are exercised by the **container gates** (the Testcontainers
migration up→down→up + idempotency tests) and the **live replay** (handbook §1), not by unit tests:

- **Live-IO methods:** `CurationEngine.RunAsync`/`EnsureCuratedTopicsAsync`,
  `WarehouseSinkEngine.RunAsync`/`ConsumeSnapshot`, `KafkaTelemetrySink.EnsureTopicsAsync`,
  `MarquezLineageEmitter.ValidateServerCertificate` (runs only during a TLS handshake).
- **Driver/connection wrappers:** `StarRocksClient`, `TlsClickHouseConnectionFactory`.
- **Composition roots:** `IngestionModule` / `WarehouseModule` / `TelemetryModule`, the
  `CurationWorker` / `WarehouseSinkWorker` host loops, `TelemetryTopicInitializer`, `ObservabilityConsole`,
  and the console `Program` entry points (separate exe assemblies, not referenced by the test projects).

## Consequences

- **Positive:** the gate measures test quality, not generated-code noise. Logic coverage is **93% line /
  85% branch** overall, **≥80% line and branch on every logic assembly** (SharedKernel, Ingestion,
  Warehouse, Telemetry, Lineage, Clickhouse). The seams are real DIP improvements — the loaders and sinks
  no longer bind to a concrete driver. 102 unit + 6 architecture + 3 container-gate tests.
- **Negative:** the excluded IO boundaries are only covered by the container gates + live replay, so a
  regression *inside* a Kafka consume loop or the StarRocks client would not be caught by a unit test.
  That is the correct trade — those paths need real infrastructure to test meaningfully, and the
  handbook's from-zero replay + the container gates exercise them.
- **Verification:** the exclusions are auditable — every `[ExcludeFromCodeCoverage]` carries a reason,
  and `coverlet.runsettings` is committed. The CI "Coverage gate" step fails the build below 80%.
- **Canon:** satisfies MASTER-PLAN **E12** for v0.1.0.
