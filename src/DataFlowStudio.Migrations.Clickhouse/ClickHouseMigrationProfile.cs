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
    /// The lab profile: a replicated cluster. Table engines are <c>Replicated*MergeTree</c> with the
    /// Guide-13 <c>/ch/tables/{shard}/…</c> Keeper paths and <c>{replica}</c> macro, and DDL runs
    /// <c>ON CLUSTER</c> so every node materializes it.
    /// </summary>
    /// <param name="cluster">The ClickHouse cluster name (the lab's is <c>nexus_analytics</c>, per Guide 13).</param>
    public static ClickHouseMigrationProfile Lab(string cluster = "nexus_analytics") => new()
    {
        Variables = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["onCluster"] = $" ON CLUSTER {cluster}",
            ["cluster"] = cluster,
            ["engine_pipeline_events_local"] = "ReplicatedMergeTree('/ch/tables/{shard}/pipeline_events', '{replica}')",
            ["engine_cdc_lag"] = "ReplicatedMergeTree('/ch/tables/{shard}/cdc_lag', '{replica}')",
            ["engine_error_events"] = "ReplicatedMergeTree('/ch/tables/{shard}/error_events', '{replica}')",
            ["engine_latency_mv"] = "ReplicatedAggregatingMergeTree('/ch/tables/{shard}/pipeline_latency_by_hour', '{replica}')",
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
        Variables = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["onCluster"] = string.Empty,
            ["cluster"] = cluster,
            ["engine_pipeline_events_local"] = "MergeTree",
            ["engine_cdc_lag"] = "MergeTree",
            ["engine_error_events"] = "MergeTree",
            ["engine_latency_mv"] = "AggregatingMergeTree",
        },
    };
}
