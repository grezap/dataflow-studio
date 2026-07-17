-- analytics.pipeline_latency_by_hour — an AggregatingMergeTree materialized view that rolls
-- pipeline_events_local into hourly p50/p95/p99 latency + event-count aggregate states.
-- Reproduces schemas/dataflow-studio/README.md. ADR-0005: added $onCluster$ so the MV is created on
-- every node (the authored DDL omitted it), and the target engine is templated for lab vs container.

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.pipeline_latency_by_hour$onCluster$
ENGINE = $engine_latency_mv$
PARTITION BY toYYYYMM(hour) ORDER BY (pipeline, stage, hour)
AS
SELECT
    toStartOfHour(event_time) AS hour,
    pipeline, stage,
    quantilesState(0.5, 0.95, 0.99)(duration_ms) AS p_state,
    countState() AS events_state
FROM analytics.pipeline_events_local
GROUP BY hour, pipeline, stage;
