-- analytics.cdc_lag_seconds — end-to-end CDC lag samples per source/topic, for the freshness SLO.
-- Reproduces schemas/dataflow-studio/README.md; engine templated for lab (replicated) vs container.

CREATE TABLE IF NOT EXISTS analytics.cdc_lag_seconds$onCluster$ (
    event_time  DateTime64(3),
    source      LowCardinality(String),
    topic       LowCardinality(String),
    lag_seconds Float64
) ENGINE = $engine_cdc_lag$
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (source, topic, event_time);
