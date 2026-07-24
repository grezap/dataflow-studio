# ADR-0010 — OpenTelemetry OTLP export to the lab observability tier (E16)

- **Status:** accepted
- **Date:** 2026-07-24
- **Deciders:** Grigoris Zapantis

## Context

Session 3D wired the telemetry *seam* (`IPipelineTelemetrySink`, ADR-0008) and a single OpenTelemetry
counter, but with **no spans** and no exporter — the observability tier was off, so `AddNexusObservability`
was called only when an OTLP endpoint happened to be set and nothing exercised it. Week-3 Session 3E.2
brings the lab **Grafana LGTM** tier online (Phase 0.I: Prometheus + Loki + **Tempo** + Grafana behind an
**OTel Collector** pair), so the pipeline can now report distributed traces + metrics for real (E16).

Three facts about the live tier shaped the design (all diagnosed against the running collector, not
assumed):

1. **The collector's OTLP receiver is server-TLS only.** Both `:4317` (gRPC) and `:4318` (HTTP) present a
   private-CA server certificate but **do not request a client certificate** (`No client certificate CA
   names sent`). The earlier assumption that OTLP was mutual-TLS was a worst-case; there is no client-cert
   or Vault-issuance step.
2. **The server presents leaf + intermediate**, so a client that trusts only the NexusPlatform **root**
   completes the chain. The build host already has the root at `~/.nexus/vault-ca-bundle.crt`.
3. **OTel .NET does not append the per-signal path** (`/v1/traces`, `/v1/metrics`) when the OTLP endpoint
   is set programmatically for HTTP/protobuf — it POSTs to the bare `:4318/` and the collector 404s.

## Decision

**Export traces + metrics over OTLP HTTP/protobuf (`:4318`) through `Nexus.Observability`
(nexus-shared ≥ 0.2.0), trusting the lab PKI root for the collector's server certificate.**

- **HTTP/protobuf, not gRPC.** A private CA can only be pinned through the exporter's `HttpClientFactory`
  (a custom-root `ServerCertificateCustomValidationCallback`), which the gRPC transport does not surface.
  `Nexus.Observability` therefore defaults to HTTP/protobuf, appends the per-signal path itself, and
  validates the collector's leaf against the supplied root — see the nexus-shared 0.2.0 changelog.
- **Spans from one ActivitySource** (`DataflowActivity`, in `SharedKernel.Telemetry` so a module never
  references the Telemetry module — module isolation, ADR-0001). The curation engine emits a
  `curation.drain` root span with a per-record `curate` child; the warehouse-sink engine emits a
  `warehouse-sink.load` root with a `sink.<stage>` child per loader. Each run's OTel **trace id becomes
  the ClickHouse `pipeline_events.trace_id`**, so a run correlates across Tempo + ClickHouse.
- **The emit counter exports too.** `AddNexusObservability`'s additional-meters list registers
  `DataFlowStudio.Telemetry`, so `dfs_telemetry_emitted_records_total` (by `stream`) reaches Prometheus
  via the collector's remote-write. (This surfaced a pre-existing obs defect — Prometheus lacked
  `--web.enable-remote-write-receiver` — fixed in the observability tier, not here.)
- **Off by default, free when off.** Export is wired only when `DFS_OTLP_ENDPOINT` is set; otherwise
  `ActivitySource.StartActivity` returns null (no listeners) and the counter has no reader — zero cost.

## Consequences

- A new nexus-shared minor (0.2.0) — additive, non-breaking — extends `ObservabilityOptions`
  (`Protocol`, `ServerCaCertificates`, `AdditionalSources`/`AdditionalMeters`) and adds
  `BuildTracerProvider`/`BuildMeterProvider` for DI-less consoles.
- The consoles (`dfs-curate`, `dfs-warehouse-sink`) build the OTel providers around the drain and dispose
  them to flush; `scripts/dfs-otel-demo.ps1` drives the demo. Handbook §1.8b verifies it.
- **Live-proven** (3E.2): a curation drain produced a Tempo trace of `curation.drain` + 28 `curate` spans
  (service `dfs-curation`, resolvable in Grafana's Tempo datasource) and
  `dfs_telemetry_emitted_records_total{stream=pipeline_events}=29,{cdc_lag}=28` in Prometheus.
- **Known obs-tier caveat (not a dataflow-studio concern):** the collector remote-writes to
  `prometheus.nexus.lab`, which RR-DNS-balances two *independent* Prometheus instances with no dedup, so a
  remote-written metric lands on one node and Grafana (pinned to one) may query the other. Tracked as an
  observability-tier HA follow-up.
