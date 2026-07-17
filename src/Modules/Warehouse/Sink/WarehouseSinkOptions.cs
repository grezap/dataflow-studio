using Nexus.Kafka;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// Connection + behaviour settings for the StarRocks DWH sink. The Kafka mTLS material lives on
/// <see cref="Kafka"/> (PEM text, resolved from Vault by the host); the StarRocks connection is a
/// MySqlConnector string to the FE query port (:9030, TLS-off — see the lab notes).
/// </summary>
public sealed record WarehouseSinkOptions
{
    /// <summary>Kafka connection (backplane bootstrap + mTLS PEMs) for consuming curated topics.</summary>
    public required KafkaConnectionOptions Kafka { get; init; }

    /// <summary>Schema Registry URL for the curated Avro deserializers.</summary>
    public required string SchemaRegistryUrl { get; init; }

    /// <summary>Whether to verify the Schema Registry TLS certificate (false in the lab).</summary>
    public bool VerifySchemaRegistryCertificate { get; init; }

    /// <summary>MySqlConnector connection string to the StarRocks FE (<c>SslMode=None</c>).</summary>
    public required string StarRocksConnection { get; init; }

    /// <summary>Consumer group. A unique group replays every curated record from the beginning (drain).</summary>
    public string ConsumerGroup { get; init; } = "dfs-warehouse-sink";

    /// <summary>In drain mode, stop once no curated record arrives for this long.</summary>
    public TimeSpan DrainIdleTimeout { get; init; } = TimeSpan.FromSeconds(15);
}
