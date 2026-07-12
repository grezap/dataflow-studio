-- analytics.error_events — structured pipeline errors (trace-correlated) for triage + alerting.
-- Reproduces schemas/dataflow-studio/README.md; engine templated for lab (replicated) vs container.

CREATE TABLE IF NOT EXISTS analytics.error_events$onCluster$ (
    event_time DateTime64(3),
    trace_id   String,
    service    LowCardinality(String),
    error_code LowCardinality(String),
    message    String,
    stack      String
) ENGINE = $engine_error_events$
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (service, error_code, event_time);
