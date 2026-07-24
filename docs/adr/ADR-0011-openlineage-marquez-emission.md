# ADR-0011 — OpenLineage emission to Marquez (E16)

- **Status:** accepted
- **Date:** 2026-07-25
- **Deciders:** Grigoris Zapantis

## Context

The pipeline moves data across five systems (OltpDb → CDC/Kafka → curated Avro → StarRocks DWH →
ClickHouse), but nothing recorded the **dataset graph** — the "if I change `OltpDb.Customers`, what
breaks downstream?" view (enhancement **E16**). Week-3 Session 3E.1 stood up **Marquez** (the
OpenLineage backend) as a first-class tier (Phase 0.Q, `nexus-infra-platform-tools`, ADR-0043); this
ADR makes dataflow-studio *emit* into it — dataflow-studio is Marquez's first real emitter.

Marquez accepts OpenLineage **RunEvents** at `POST /api/v1/lineage` behind an nginx TLS front door on
the `marquez` node. Diagnosed against the live tier (not assumed):

- The front-door leaf is a private-CA certificate (`platform-tools-server` role) that **chains to the
  NexusPlatform root** and carries an **IP SAN** (`192.168.70.127`). So a WORKGROUP build host can POST
  straight to the front door **by IP**, validating the leaf against the lab root — the same custom-root
  trust the OTLP exporter uses (ADR-0010). No client certificate is required.
- There is no official OpenLineage **.NET** client, so the RunEvent wire shape is hand-rolled (matching
  the platform-tools `marquez-lineage-demo`).

## Decision

**Emit OpenLineage START/COMPLETE run events per pipeline job from a small `DataFlowStudio.Lineage`
library, behind an `ILineageEmitter` seam in the SharedKernel.**

- **The seam lives in SharedKernel** (`ILineageEmitter` + `NullLineageEmitter`), so the Ingestion and
  Warehouse engines depend on the abstraction and never reference the concrete emitter — module
  isolation (ADR-0001), exactly like `IPipelineTelemetrySink`. Datasets are passed as **names**; the
  emitter places them all in its configured namespace, so the raw → curated → DWH graph is one connected
  lineage.
- **The concrete `MarquezLineageEmitter` lives in `DataFlowStudio.Lineage`** — a focused BCL-only library
  (HttpClient + source-generated `System.Text.Json`) kept out of SharedKernel, mirroring
  `DataFlowStudio.Clickhouse`. It POSTs to the front door by IP, pins the leaf to the lab root, and is
  **best-effort**: an unreachable or erroring Marquez is logged and swallowed so a lineage side-channel
  can never fail the pipeline.
- **Two jobs, dataset-level I/O.** The curation run is the `curation` job (inputs: the 10 raw CDC topics;
  outputs: the 10 curated topics); the warehouse-sink run is the `warehouse-sink` job (inputs: the 10
  curated topics; outputs: the 9 DWH tables). `dfs-trace` Face 5 additionally emits a `dfs-trace` job for
  the single traced record's path. Job-level (not per-record) events keep the graph readable.
- **The runId is the run's OpenTelemetry trace id (as a UUID)**, so a single run is one correlated entity
  across **three planes** — a Tempo trace (ADR-0010), ClickHouse `pipeline_events` (ADR-0008), and a
  Marquez run.
- **Off by default, free when off.** Emission is wired only when `DFS_MARQUEZ_ENDPOINT` is set; otherwise
  the engines resolve `NullLineageEmitter` and nothing is emitted.

## Consequences

- New `DataFlowStudio.Lineage` assembly + a `SharedKernel.Lineage` seam; the consoles + `dfs-trace` build
  the emitter via `LineageEmitterFactory` (`DFS_MARQUEZ_ENDPOINT` / `DFS_MARQUEZ_CACERT` /
  `DFS_MARQUEZ_NAMESPACE`), the Api host via a DI registration. `scripts/dfs-lineage-demo.ps1` drives it
  and reads the graph back; handbook §1.8c verifies it.
- **Live-proven (3F):** curation + warehouse-sink emitted the full graph into Marquez namespace
  `dataflow-studio` — **2 jobs + 29 datasets** (10 raw + 10 curated + 9 DWH); the downstream query from
  `oltp.OltpDb.dbo.Customers` returns the entire curated + DWH layer. No client cert; the front-door leaf
  validated against the lab root by IP SAN.
- Lineage facets (schema, columnLineage, run duration) are deliberately minimal here — the dataset graph
  is the E16 goal; richer facets are a later enhancement.
