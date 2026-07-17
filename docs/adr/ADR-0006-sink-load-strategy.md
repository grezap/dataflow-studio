# ADR-0006 — Sink load strategy (StarRocks DWH + ClickHouse telemetry)

- **Status:** accepted
- **Date:** 2026-07-17
- **Deciders:** Grigoris Zapantis

## Context

The curated Avro topics must land in two analytical stores: the StarRocks Kimball DWH (SCD2
dimensions + facts) and the ClickHouse telemetry schema. The Week-3 kickoff decision (D1) chose
**both** load styles — native engine ingestion where it fits, a .NET worker where control is needed.

## Decision

**Split by store, matching each to the load style it suits.**

### StarRocks DWH — a .NET Warehouse sink worker
The Warehouse module consumes the curated topics and loads the star itself, because SCD2 and
surrogate-key management need row-level control that a native routine load does not give:

- **SCD2 for `dim_customer`/`dim_product`** — compare each record's attribute signature to the current
  version; unchanged → skip (idempotent); changed/absent → `UPDATE` the old row closed
  (`is_current=0`, `valid_to=now`) and `INSERT` a new version with a worker-generated surrogate key
  (`MAX(sk)+1`). **SCD1 upsert** for `dim_warehouse`/`dim_carrier`; **generated** `dim_date`.
- **Facts** truncate-and-reload the current snapshot (idempotent), creating the range partitions each
  batch needs (`ADD PARTITION IF NOT EXISTS`). Per-change incremental fact loading is a later
  enhancement. `fact_order_line.line_total_usd` is **recomputed** here (the source column is computed
  and comes through CDC as NULL — see ADR-0007).
- **Loads over the MySQL wire (MySqlConnector), batched.** StarRocks creates a new version per INSERT,
  so every loader emits a **single multi-row INSERT** per table — batching is required, not just an
  optimisation. Stream Load (HTTP bulk) is the alternative for high volume; the seed-scale batched
  INSERT is simpler and sufficient here.
- **At-least-once → dedup by message key.** The curated topics can carry duplicates; the sink keeps
  the latest record per Kafka key before loading, and every load path is idempotent regardless.

### ClickHouse telemetry — native Kafka-engine ingestion (Session 3D)
The telemetry tables are append-only, high-volume time-series with no SCD logic — a natural fit for
ClickHouse's **Kafka engine + materialized view** to pull directly from Kafka. The .NET Telemetry
worker owns only what native ingestion cannot (e.g. `error_events` structured inserts). This is the
"native" half of D1 and lands in Session 3D.

### Ordering
The drain consumes all curated topics into memory, then loads in dependency order — dimensions before
facts, categories before products, orders before order lines — so every surrogate-key lookup is ready
when a fact needs it. A runnable `dfs warehouse-sink` console (drain) and the hosted
`WarehouseSinkWorker` (on a timer) share one engine.

## Consequences

- **Positive:** full SCD2 control + a clean, idempotent, batched load; the native ClickHouse path
  keeps the telemetry firehose off the .NET heap. One engine serves the console, the demo, and the worker.
- **Negative:** facts are a full-snapshot reload in drain mode (incremental is deferred); surrogate
  keys are worker-generated (a single loader instance per run — no concurrent writers). Accepted for
  the portfolio scope.
