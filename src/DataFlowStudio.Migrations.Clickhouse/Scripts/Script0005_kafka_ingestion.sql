-- analytics — native Kafka-engine telemetry ingestion (ADR-0008 / Session 3D).
-- Three Kafka-engine source tables consume the dfs.telemetry.* topics as JSONEachRow; a materialized
-- view per stream converts the epoch-millisecond event_ms to DateTime64(3) and pushes each row into
-- the matching local telemetry table (pipeline_events_local / cdc_lag_seconds / error_events). This is
-- the "native" half of the D1 load strategy: the telemetry firehose lands in ClickHouse without a .NET
-- consumer on the path.
--
-- Lab-only: a CI single-node container has no Kafka broker, so the SingleNode profile excludes this
-- script (the E1 idempotency gate still creates the rest of the schema). The Kafka client's mTLS
-- material + security_protocol=ssl live in the ClickHouse server <kafka> config on each data node
-- (issued from Vault) — never in this DDL. See handbook 3d / Guide 23 section 9.

CREATE TABLE IF NOT EXISTS analytics.pipeline_events_kafka$onCluster$ (
    event_ms    Int64,
    trace_id    String,
    pipeline    String,
    stage       String,
    status      String,
    duration_ms UInt32,
    payload     String
) ENGINE = Kafka SETTINGS
    kafka_broker_list = '$kafka_brokers$',
    kafka_topic_list = 'dfs.telemetry.pipeline_events',
    kafka_group_name = '$kafka_group_pipeline_events$',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.pipeline_events_kafka_mv$onCluster$
TO analytics.pipeline_events_local AS
SELECT
    fromUnixTimestamp64Milli(event_ms) AS event_time,
    trace_id, pipeline, stage, status, duration_ms, payload
FROM analytics.pipeline_events_kafka;

CREATE TABLE IF NOT EXISTS analytics.cdc_lag_seconds_kafka$onCluster$ (
    event_ms    Int64,
    source      String,
    topic       String,
    lag_seconds Float64
) ENGINE = Kafka SETTINGS
    kafka_broker_list = '$kafka_brokers$',
    kafka_topic_list = 'dfs.telemetry.cdc_lag',
    kafka_group_name = '$kafka_group_cdc_lag$',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.cdc_lag_seconds_kafka_mv$onCluster$
TO analytics.cdc_lag_seconds AS
SELECT
    fromUnixTimestamp64Milli(event_ms) AS event_time,
    source, topic, lag_seconds
FROM analytics.cdc_lag_seconds_kafka;

CREATE TABLE IF NOT EXISTS analytics.error_events_kafka$onCluster$ (
    event_ms   Int64,
    trace_id   String,
    service    String,
    error_code String,
    message    String,
    stack      String
) ENGINE = Kafka SETTINGS
    kafka_broker_list = '$kafka_brokers$',
    kafka_topic_list = 'dfs.telemetry.error_events',
    kafka_group_name = '$kafka_group_error_events$',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.error_events_kafka_mv$onCluster$
TO analytics.error_events AS
SELECT
    fromUnixTimestamp64Milli(event_ms) AS event_time,
    trace_id, service, error_code, message, stack
FROM analytics.error_events_kafka;
