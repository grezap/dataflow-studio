# DEMO-01 · CDC to DWH — one change, three observation planes

> DataFlow Studio's own playbook (the project-local companion to the platform-level
> `nexus-platform-plan` DEMO-01). Every step here is **replayable from zero today** with the committed
> `scripts/dfs-*.ps1`; the from-zero detail is [`handbook.md`](../handbook.md) §1.

## 1. What this shows

A single edit to a customer row in the OLTP source of truth (`OltpDb`) travels — via log-based CDC — into
a clean, schema-governed curated event, then into a Kimball warehouse as a **new SCD2 version**, while the
pipeline **observes itself across three planes** (Tempo traces, ClickHouse `pipeline_events`, Marquez
lineage) that all share one correlation id. The insight a viewer leaves with: *a production-shaped CDC
pipeline is not just plumbing — it is governed (Avro contracts), correct (idempotent SCD2), and
observable (one run = one id across three planes).* Personas: the **data architect** (governance + star
schema) and the **platform/SRE engineer** (self-observation).

## 2. Runtime + prerequisites

- **Environment target** — `data-engineering`.
- **VMs required** (`docs/infra/vms.yaml`): SQL AG `sql-fci-1/2` + `sql-ag-rep-1/2` (.11–.14, listener .17),
  `kafka-east-1/2/3` (.21–.23), `schema-registry-1/2` (.91/.92), `kafka-connect-1/2` (.95/.96),
  StarRocks-SN `sr-fe-*` + `sr-be-*` (.31–.36), ClickHouse `ch-*` (.41–.49); for the planes: obs LGTM
  (.170–.183), `marquez` (.127/.134/.135).
- **External services** — raw topics `oltp.OltpDb.dbo.*`, curated topics `dfs.<entity>.changed.v1`,
  telemetry topics `dfs.telemetry.*`; Schema Registry `https://192.168.10.91:8081`; StarRocks `dwh` +
  `analytics`; ClickHouse `analytics`; Marquez namespace `dataflow-studio`.
- **Seed data** — the embedded generator: `.\scripts\dfs-seed.ps1` (idempotent; ~59 curated records).
- **Expected duration** — ~5 min (seed → curate → sink → verify).
- **Reset command** — `nexus-cli demo run DEMO-DFS-01 --reset` (re-seeds + re-drains; every load is idempotent).

## 3. Architecture snapshot

```
OltpDb (SQL Server AG, listener .17)     source of truth — 11 tables, temporal + audit cols
   │  SQL Server CDC (log-based) → Debezium (Kafka Connect .95/.96)
   ▼
oltp.OltpDb.dbo.*  (raw JSON CDC)        10 tables → raw Kafka topics
   │  .NET curation worker (Ingestion module, data-driven catalog — ADR-0007)
   ▼
dfs.<entity>.changed.v1 (curated Avro)   10 typed, versioned contracts (Schema Registry)
   │  .NET Warehouse sink (ADR-0006)                    │ telemetry from BOTH stages → dfs.telemetry.*
   ▼                                                    ▼
StarRocks dwh (Kimball star)             ClickHouse analytics (native Kafka-engine ingestion — ADR-0008)
   SCD2 dim_customer/dim_product + 4 facts   pipeline_events · cdc_lag_seconds · error_events
   │
   └── silver-layer PySpark conformance job (pyspark/, ADR-0014) reads the star → conformed Parquet
```

Three planes, one id: the run's **OTel trace id** == the ClickHouse `pipeline_events.trace_id` == the
Marquez `runId`.

## 4. Step-by-step script

1. **Seed the source.** `.\scripts\dfs-seed.ps1`
   **Expected observable.** OltpDb populated (4 customers, 6 products, 4 orders, …); the marker row
   `SEED-C001` short-circuits a re-run. CDC streams the inserts to the raw topics.
2. **Curate.** `.\scripts\dfs-curate.ps1`
   **Expected observable.** `Curated records per entity` prints ~59 across the 10 `dfs.*.changed.v1`
   topics; all 10 Schema Registry subjects registered.
3. **Load the star.** `.\scripts\dfs-warehouse-sink.ps1`
   **Expected observable.** `dim_customer` (8 current), `dim_product` (6), the 3 SCD1 dims, and 4 facts
   loaded; a re-run is a no-op (SCD2 skips unchanged).
4. **Edit a customer, watch an SCD2 version appear.** Update a customer's email in OltpDb, re-run
   curate + sink. **Expected observable.** `dim_customer` gains one row: the old version closed
   (`is_current = 0`, `valid_to` set), a new current version inserted. (Detail: [`watch-the-pipeline.md`](watch-the-pipeline.md).)
5. **Follow one record across the faces.** `.\scripts\dfs-trace.ps1`
   **Expected observable.** The five faces of one record: OLTP write → CDC → raw Debezium → curated Avro
   → sink projection.
6. **Read the telemetry.** `.\scripts\dfs-telemetry.ps1 all`
   **Expected observable.** `pipeline_events`, `cdc_lag_seconds`, `error_events` populated; the
   latency MV returns p50/p95/p99 per stage.
7. **See the lineage.** `.\scripts\dfs-lineage-demo.ps1`
   **Expected observable.** The Marquez `dataflow-studio` namespace shows 2 jobs + 29 datasets; the
   downstream query from `oltp.OltpDb.dbo.Customers` returns the whole curated + DWH layer.
8. **Conform in the silver layer.** `uv run --project pyspark dfs-conform --source parquet ...`
   **Expected observable.** The PySpark job reads the star, computes the customer-360 conformed table,
   and writes Parquet; the run summary prints row counts (see [`pyspark/README.md`](../../pyspark/README.md)).

## 5. Observability trail

- **Tempo (traces)** — service `dfs-curation` / `dfs-warehouse-sink`; a `curation.drain` root + one
  `curate` per record. Resolve via Grafana's Tempo datasource (`rootServiceName=dfs-curation`).
  URL: `https://grafana.nexus.lab` (handbook §1.8b).
- **ClickHouse (`analytics`)** — `pipeline_events` (per-stage latency), `cdc_lag_seconds`, `error_events`;
  `pipeline_latency_by_hour` MV for p50/p95/p99 (handbook §1.8a).
- **Prometheus** — `dfs_telemetry_emitted_records_total{stream=…}` (query both proms — obs-HA caveat).
- **Marquez (OpenLineage)** — namespace `dataflow-studio`, jobs `curation` + `warehouse-sink`; the
  `oltp.* → dfs.* → dwh.*` dataset graph. `POST /api/v1/lineage` front door `https://192.168.70.127`.

## 6. Code pointers

- `src/Modules/Ingestion/Curation/` — the data-driven curation catalog + projector (ADR-0007).
- `src/Modules/Warehouse/Sink/DimensionLoader.cs` / `FactLoader.cs` — SCD2 dims + facts (ADR-0006).
- `src/Modules/Telemetry/` — the Kafka + ClickHouse telemetry sinks (ADR-0008).
- `src/DataFlowStudio.Lineage/MarquezLineageEmitter.cs` — OpenLineage emission (ADR-0011).
- `pyspark/src/dfs_conform/` — the silver-layer conformance job (ADR-0014).

## 7. Variations

- **Fresh vs incremental** — a fresh seed loads the full snapshot; an edit-then-replay shows the SCD2
  delta only. **No planes** — omit `DFS_OTLP_ENDPOINT` / `DFS_MARQUEZ_ENDPOINT` and the run still
  completes (the seams no-op). **Error paths** — `dfs-telemetry.ps1 demo-errors` emits one error down
  each path (native Kafka-engine + direct HTTPS) and polls until both land.

## 8. Troubleshooting

- **Marquez `:443` times out on a resumed VM** — the docker stack is wedged + DNAT flushed;
  `sudo systemctl restart docker` on `.127` (handbook T30).
- **StarRocks "FE not ready"** — `root` has a password; a password-less probe fails silently.
- **🔴 Panic button** (handbook §3.4): pause CDC (`curl -sk -X PUT https://localhost:8083/connectors/oltp-cdc/pause`),
  stop the workers, and re-run the sink to converge — every domain-data load is idempotent.

## 9. What this proves

- **.NET engineering + architecture** — modular monolith with NetArchTest-enforced boundaries; DIP seams
  (`IStarRocksClient`, `IErrorFallbackSink`) tested to ≥80% line+branch (ADR-0012); no EF Core on the CDC
  paths (ADR-0007).
- **Advanced SQL + analytics** — SQL Server CDC + temporal tables; StarRocks SCD2 (PK-model `UPDATE` +
  batched `INSERT`, no `MERGE`); ClickHouse aggregating MVs for p50/p95/p99.
- **Python** — the `pyspark/` silver-layer conformance job (uv + Ruff + mypy --strict + Pydantic v2),
  reading the Kimball star into a conformed customer-360 Parquet (ADR-0014).
- **DevOps** — three observation planes correlated by one id; container packaging (Docker/compose/Swarm/K8s)
  + the Aspire AppHost; `nexus-cli deploy dataflow-studio` + `nexus-cli demo run DEMO-DFS-01`.
