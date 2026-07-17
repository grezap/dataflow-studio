-- analytics.pipeline_events — per-stage pipeline telemetry, sharded + replicated across the cluster.
-- Reproduces schemas/dataflow-studio/README.md. ADR-0005 fix: the authored DDL named the cluster
-- `nexus_ch`; the lab cluster is `nexus_analytics` (Guide 13), injected via $cluster$ / $onCluster$.
-- The engine is templated so the same script runs on the replicated lab and a single-node container.

CREATE TABLE IF NOT EXISTS analytics.pipeline_events_local$onCluster$ (
    event_time  DateTime64(3),
    trace_id    String,
    pipeline    LowCardinality(String),
    stage       LowCardinality(String),
    status      LowCardinality(String),
    duration_ms UInt32,
    payload     String
) ENGINE = $engine_pipeline_events_local$
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (pipeline, stage, event_time);

CREATE TABLE IF NOT EXISTS analytics.pipeline_events$onCluster$ AS analytics.pipeline_events_local
ENGINE = Distributed($cluster$, analytics, pipeline_events_local, rand());
