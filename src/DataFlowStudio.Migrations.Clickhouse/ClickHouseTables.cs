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
    /// <summary>Every table + materialized view the scripts create in <c>analytics</c>.</summary>
    public static readonly IReadOnlyList<string> All =
    [
        "pipeline_events_local",       // ReplicatedMergeTree storage
        "pipeline_events",             // Distributed fan-out over the shards
        "pipeline_latency_by_hour",    // AggregatingMergeTree materialized view
        "cdc_lag_seconds",
        "error_events",
    ];
}
