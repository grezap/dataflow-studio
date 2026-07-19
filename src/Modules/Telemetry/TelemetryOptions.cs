using Nexus.Kafka;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Connection + behaviour settings for the telemetry sink. The Kafka mTLS material (carried as PEM
/// text on <see cref="Kafka"/>) drives the native-ingestion producer; the ClickHouse fields drive the
/// direct-HTTPS error inserter (the control/fallback path). All are resolved by the host from the
/// environment / Vault — never hard-coded. When <see cref="ClickHouseConnectionString"/> is null the
/// error fallback is inert (errors flow only through the native Kafka path).
/// </summary>
public sealed record TelemetryOptions
{
    /// <summary>The Kafka connection (backplane bootstrap + mTLS PEMs) for producing telemetry.</summary>
    public required KafkaConnectionOptions Kafka { get; init; }

    /// <summary>Replication factor for the auto-created <c>dfs.telemetry.*</c> topics (brokers have auto-create off).</summary>
    public short ReplicationFactor { get; init; } = 3;

    /// <summary>The ClickHouse.Client connection string for the direct-HTTPS error path; null disables it.</summary>
    public string? ClickHouseConnectionString { get; init; }

    /// <summary>Path to the ClickHouse private-CA PEM bundle (root + intermediate); null uses OS trust.</summary>
    public string? ClickHouseCaCertPath { get; init; }

    /// <summary>Optional PKCS#12 client certificate path for ClickHouse mTLS (not required for HTTPS password auth).</summary>
    public string? ClickHouseClientPfxPath { get; init; }

    /// <summary>Optional password for <see cref="ClickHouseClientPfxPath"/>.</summary>
    public string? ClickHouseClientPfxPassword { get; init; }

    /// <summary>The OTel service name stamped on spans/metrics (E16).</summary>
    public string ServiceName { get; init; } = "dataflow-studio";

    /// <summary>The OTLP collector endpoint; null when the observability tier is off (exporters no-op).</summary>
    public Uri? OtlpEndpoint { get; init; }
}
