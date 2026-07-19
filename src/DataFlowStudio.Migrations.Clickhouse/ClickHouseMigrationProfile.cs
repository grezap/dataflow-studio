namespace DataFlowStudio.Migrations.Clickhouse;

/// <summary>
/// The substitution variables that adapt the one shared script set to a target topology. The lab is
/// a replicated multi-node <c>nexus_analytics</c> cluster (ReplicatedMergeTree + <c>ON CLUSTER</c>);
/// a CI/test container is a single node with no ClickHouse Keeper (plain MergeTree, no
/// <c>ON CLUSTER</c>) but the same <c>nexus_analytics</c> cluster name defined so the
/// <c>Distributed</c> table still resolves. Every other line of DDL is identical between the two.
/// </summary>
public sealed class ClickHouseMigrationProfile
{
    /// <summary>The <c>$name$</c> tokens the runner substitutes into each script before executing it.</summary>
    public required IReadOnlyDictionary<string, string> Variables { get; init; }

    /// <summary>
    /// Script file names this topology does not apply. The Kafka-engine ingestion script
    /// (<c>Script0005</c>) is excluded on <see cref="SingleNode"/> — a lone CI container has no Kafka
    /// broker to consume from — so the E1 idempotency gate stays green while the lab still creates the
    /// native-ingestion tables (ADR-0008).
    /// </summary>
    public IReadOnlyCollection<string> ExcludedScripts { get; init; } = [];

    /// <summary>
    /// The lab profile: a replicated cluster. Table engines are <c>Replicated*MergeTree</c> with the
    /// Guide-13 <c>/ch/tables/{shard}/…</c> Keeper paths and <c>{replica}</c> macro, and DDL runs
    /// <c>ON CLUSTER</c> so every node materializes it.
    /// </summary>
    /// <param name="cluster">The ClickHouse cluster name (the lab's is <c>nexus_analytics</c>, per Guide 13).</param>
    /// <param name="kafkaBrokers">Comma-separated broker list the Kafka-engine tables consume from (backplane mTLS).</param>
    /// <param name="kafkaGroupPrefix">Consumer-group prefix for the three telemetry streams (matches the ClickHouse Kafka ACL).</param>
    public static ClickHouseMigrationProfile Lab(
        string cluster = "nexus_analytics",
        string kafkaBrokers = "192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092",
        string kafkaGroupPrefix = "dfs-clickhouse") => new()
        {
            Variables = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["onCluster"] = $" ON CLUSTER {cluster}",
                ["cluster"] = cluster,
                ["engine_pipeline_events_local"] = "ReplicatedMergeTree('/ch/tables/{shard}/pipeline_events', '{replica}')",
                ["engine_cdc_lag"] = "ReplicatedMergeTree('/ch/tables/{shard}/cdc_lag', '{replica}')",
                ["engine_error_events"] = "ReplicatedMergeTree('/ch/tables/{shard}/error_events', '{replica}')",
                ["engine_latency_mv"] = "ReplicatedAggregatingMergeTree('/ch/tables/{shard}/pipeline_latency_by_hour', '{replica}')",
                ["kafka_brokers"] = kafkaBrokers,
                ["kafka_group_pipeline_events"] = $"{kafkaGroupPrefix}-pipeline-events",
                ["kafka_group_cdc_lag"] = $"{kafkaGroupPrefix}-cdc-lag",
                ["kafka_group_error_events"] = $"{kafkaGroupPrefix}-error-events",
            },
        };

    /// <summary>
    /// The single-node profile for CI/tests: plain (non-replicated) engines and no <c>ON CLUSTER</c>
    /// (a lone container has no Keeper to coordinate distributed DDL), but the same cluster name so
    /// the <c>Distributed</c> table resolves against the one-node cluster the container defines.
    /// </summary>
    /// <param name="cluster">The single-node cluster name defined in the container config (default <c>nexus_analytics</c>).</param>
    public static ClickHouseMigrationProfile SingleNode(string cluster = "nexus_analytics") => new()
    {
        // A lone container has no Kafka broker, so the native Kafka-engine script never runs here.
        ExcludedScripts = ["Script0005_kafka_ingestion.sql"],
        Variables = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["onCluster"] = string.Empty,
            ["cluster"] = cluster,
            ["engine_pipeline_events_local"] = "MergeTree",
            ["engine_cdc_lag"] = "MergeTree",
            ["engine_error_events"] = "MergeTree",
            ["engine_latency_mv"] = "AggregatingMergeTree",
            // Present but unused (Script0005 is excluded) so the profile is self-contained.
            ["kafka_brokers"] = "localhost:9092",
            ["kafka_group_pipeline_events"] = "dfs-clickhouse-pipeline-events",
            ["kafka_group_cdc_lag"] = "dfs-clickhouse-cdc-lag",
            ["kafka_group_error_events"] = "dfs-clickhouse-error-events",
        },
    };
}
