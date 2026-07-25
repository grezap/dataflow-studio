# ADR-0013 — .NET Aspire AppHost for local orchestration (Api + pipeline consoles)

- **Status:** accepted
- **Date:** 2026-07-25
- **Deciders:** Grigoris Zapantis

## Context

DataFlow Studio ships one deployable Api (the modular monolith) plus a handful of runnable consoles that
drive the pipeline against the lab tiers — `Seed`, `Curation`, `WarehouseSink`, `Trace`, and the
`Telemetry` verify console. Running the end-to-end flow means launching several processes in the right
order with the same environment wiring (Kafka mTLS PEM paths, the StarRocks / ClickHouse connections, the
OTLP + Marquez endpoints). Week 4 wants a single, discoverable entry point for that — and a dashboard that
shows the whole topology, each resource's logs, and the Api's endpoints — without inventing a bespoke
launcher.

## Decision

**Add a `DataFlowStudio.AppHost` (.NET Aspire 13.4.6, net10) that composes the Api as an always-on
resource and models each pipeline console as a first-class, explicit-start resource.**

- **The Api** is the always-on node: `AddProject<Projects.DataFlowStudio_Api>("dfs-api")`. The Aspire
  dashboard shows its HTTP endpoints (`/health`, `/modules`), structured logs, and (with the pipeline env
  wired) its hosted curation + warehouse-sink workers.
- **The consoles** — `dfs-seed`, `dfs-curation`, `dfs-warehouse-sink`, `dfs-trace`, `dfs-telemetry-verify`
  — are added as resources with **`.WithExplicitStart()`**. They are drain / demo jobs (each runs once,
  then exits), so auto-starting them at boot would just show five processes that immediately exit
  "not configured". Explicit-start makes them visible in the topology and launchable on demand, in order.
- **Configuration flows through once.** The AppHost forwards whatever `DFS_*` variables are set in its own
  environment to every composed resource, so the orchestrated run uses the exact same wiring (and the same
  Vault-issued secrets) as the standalone consoles — nothing is duplicated or hard-coded.

### What was deliberately *not* done

- **No `ServiceDefaults` project / no change to the Api's telemetry.** The Api already exports
  OpenTelemetry to the lab LGTM tier via `Nexus.Observability` when `DFS_OTLP_ENDPOINT` is set
  (ADR-0010). Adding Aspire's `AddServiceDefaults()` would stand up a *second* tracer/meter provider in
  the same process (pointed at the dashboard's OTLP endpoint) — two competing OTel pipelines. The pipeline
  telemetry story is already told by the lab tier + the ClickHouse/Marquez planes, so the AppHost stays an
  orchestrator and does not touch module or Api code. **Module isolation and the no-EF-on-AOT invariant
  (ADR-0001 / ADR-0007) are therefore untouched** — the AppHost references only the runnable projects.
- **No container resources.** Kafka, StarRocks, ClickHouse, the collector, and Marquez are the lab tiers
  (or, for a laptop, the `docker-compose` in `deploy/` — ADR-0014); the AppHost orchestrates *this*
  application's processes, not the platform it runs on.

## Consequences

- **Positive:** `dotnet run --project src/DataFlowStudio.AppHost` brings up the Aspire dashboard with the
  whole application topology, per-resource logs, and the Api's live endpoints — one command, one place. The
  console ordering (seed → curate → warehouse-sink) is discoverable rather than tribal knowledge.
- **Negative:** the AppHost is a net10 Aspire host, so it needs the Aspire workload-free SDK
  (`Aspire.AppHost.Sdk`, restored from NuGet — no separate workload install) and the dashboard is a
  developer convenience, not a production surface. Production deployment is the container images +
  compose / K8s manifests (ADR-0014), not the AppHost.
- **Verification:** the AppHost builds clean under warnings-as-errors and is part of the solution build.
  The composed projects are the same ones the container images package, so the topology the dashboard
  shows matches what ships.
