namespace DataFlowStudio.Migrations.Clickhouse;

/// <summary>
/// The canonical list of objects the ClickHouse migration scripts create in the <c>analytics</c>
/// database, used by the idempotency test to assert the telemetry schema is present after
/// <c>MigrateUp</c>. Mirrors the authored DDL in <c>schemas/dataflow-studio/README.md</c> (with the
/// <c>nexus_ch</c> → <c>nexus_analytics</c> cluster fix recorded in ADR-0005). Materialized views
/// surface in <c>system.tables</c> alongside tables, so the count is over both.
/// </summary>
public static class ClickHouseTables
{
    /// <summary>
    /// Every table + materialized view the topology-agnostic scripts (0001–0004) create in
    /// <c>analytics</c>. Present on both the replicated lab and a single-node container — the E1 gate
    /// asserts this set. The Kafka-engine objects (Script0005) are lab-only; see <see cref="KafkaIngestion"/>.
    /// </summary>
    public static readonly IReadOnlyList<string> All =
    [
        "pipeline_events_local",       // ReplicatedMergeTree storage
        "pipeline_events",             // Distributed fan-out over the shards
        "pipeline_latency_by_hour",    // AggregatingMergeTree materialized view
        "cdc_lag_seconds",
        "error_events",
    ];

    /// <summary>
    /// The native Kafka-engine ingestion objects created by <c>Script0005</c> (lab-only — a CI
    /// single-node container has no Kafka broker). Each source table feeds its local telemetry table
    /// through the paired <c>_mv</c> materialized view (ADR-0008). Asserted by the live lab check.
    /// </summary>
    public static readonly IReadOnlyList<string> KafkaIngestion =
    [
        "pipeline_events_kafka",       "pipeline_events_kafka_mv",
        "cdc_lag_seconds_kafka",       "cdc_lag_seconds_kafka_mv",
        "error_events_kafka",          "error_events_kafka_mv",
    ];
}
