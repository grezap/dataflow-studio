# DataFlow Studio — portfolio walkthrough (v0.1.0)

A five-minute tour of what DataFlow Studio *is* and how it proves itself. The scripted version is
[`scripts/demo.tape`](../scripts/demo.tape) (render with [VHS](https://github.com/charmbracelet/vhs) →
`docs/media/dataflow-studio-demo.gif`); the data-flow detail lives in
[`watch-the-pipeline.md`](demos/watch-the-pipeline.md) and the from-zero replay in [`handbook.md`](handbook.md).

> **The persona.** A senior data engineer evaluating the project. They want to see, quickly: is it well
> engineered, is it actually tested, can it run, and does it do something real?

## 1. One solution, clean seams

DataFlow Studio is a **modular monolith** — one deployable, but the module boundaries (Commerce,
Ingestion, Warehouse, Telemetry) are enforced by architecture tests, so it can split into services later
without a rewrite. It builds under warnings-as-errors with XML docs required on every public member.

```bash
dotnet build DataFlowStudio.slnx -c Release
```

## 2. Tested to the gate (E12)

**113 tests** — 102 unit, 6 architecture (module isolation + no-EF-on-AOT), 2 Api boot-smoke, 3
container-gated migration tests. Logic coverage is **93% line / 85% branch**, ≥80/80 on every logic
assembly, enforced in CI. The loaders, sinks, and emitters are unit-tested through DIP seams
(`IStarRocksClient`, `IErrorFallbackSink`, an injectable producer + HTTP handler) rather than live
infrastructure — see [ADR-0012](adr/ADR-0012-test-coverage-strategy.md).

```bash
dotnet test tests/DataFlowStudio.UnitTests -c Release
```

## 3. Orchestrated (.NET Aspire)

One command brings up the whole topology under the Aspire dashboard — the Api always-on, and each
pipeline console (`dfs-seed`, `dfs-curation`, `dfs-warehouse-sink`, `dfs-trace`, `dfs-telemetry-verify`)
as a start-on-demand resource ([ADR-0013](adr/ADR-0013-aspire-apphost-orchestration.md)).

```bash
dotnet run --project src/DataFlowStudio.AppHost   # -> the Aspire dashboard
```

## 4. Packaged (Docker / compose / K8s)

Every host builds into a **non-root** container from one parameterized `Dockerfile`; the Api serves
`/health` with all four modules wired. There is a `docker-compose.yml` for a laptop and `k8s/` manifests
(non-root, read-only rootfs, `/health` probes) for a cluster — see [`deploy/`](../deploy/README.md).

```bash
docker run -d --rm -p 8081:8080 dfs-api:latest
curl -s localhost:8081/health    # {"status":"healthy","moduleCount":4}
curl -s localhost:8081/modules   # ["commerce","ingestion","warehouse","telemetry"]
```

## 5. It does something real — and watches itself do it

The pipeline is `OltpDb → CDC (Debezium) → curated Avro (Schema Registry) → StarRocks Kimball DWH +
ClickHouse telemetry`. Every run is **one correlated entity across three self-observation planes**:

| Plane | What it shows | Correlated by |
|---|---|---|
| **Tempo** (OTel traces) | the run's span waterfall (`curation.drain` + one `curate` per record) | the OTel **trace id** |
| **ClickHouse** `pipeline_events` | per-stage latency, CDC lag, error events | `trace_id` == the OTel trace id |
| **Marquez** (OpenLineage) | the raw → curated → DWH lineage graph (2 jobs, 29 datasets) | `runId` == the OTel trace id |

The System-A platform tour (SRE persona) is `nexus-platform-plan` **DEMO-01**; the full CDC → DWH replay,
including "edit a customer → watch an SCD2 version appear", is in [`watch-the-pipeline.md`](demos/watch-the-pipeline.md).
