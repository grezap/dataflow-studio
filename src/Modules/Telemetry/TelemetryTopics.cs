namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// The Kafka topics the pipeline produces self-observation telemetry to. ClickHouse's Kafka-engine
/// source tables consume these natively (JSONEachRow) into the <c>analytics</c> telemetry tables
/// (ADR-0008 / AsyncAPI <c>dfs.telemetry.*</c> channels). All three share the <c>dfs.telemetry</c>
/// prefix so a single topic-prefix ACL authorizes the ClickHouse consumer.
/// </summary>
public static class TelemetryTopics
{
    /// <summary>Per-stage pipeline events → <c>analytics.pipeline_events</c>.</summary>
    public const string PipelineEvents = "dfs.telemetry.pipeline_events";

    /// <summary>End-to-end CDC-lag samples → <c>analytics.cdc_lag_seconds</c>.</summary>
    public const string CdcLag = "dfs.telemetry.cdc_lag";

    /// <summary>Structured pipeline errors → <c>analytics.error_events</c>.</summary>
    public const string ErrorEvents = "dfs.telemetry.error_events";

    /// <summary>All three telemetry topics (for topic creation).</summary>
    public static readonly IReadOnlyList<string> All = [PipelineEvents, CdcLag, ErrorEvents];
}
