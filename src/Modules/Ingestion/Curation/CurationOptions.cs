using Nexus.Kafka;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// Connection + behaviour settings for the curation engine. Secrets (the Kafka mTLS material) are
/// carried as PEM text on <see cref="Kafka"/>, resolved by the host from the environment / Vault —
/// never hard-coded.
/// </summary>
public sealed record CurationOptions
{
    /// <summary>The Kafka connection (backplane bootstrap + mTLS PEMs).</summary>
    public required KafkaConnectionOptions Kafka { get; init; }

    /// <summary>The Schema Registry URL the curated Avro schemas register against.</summary>
    public required string SchemaRegistryUrl { get; init; }

    /// <summary>Whether to verify the Schema Registry TLS certificate (false in the lab; its cert has no client-trusted chain).</summary>
    public bool VerifySchemaRegistryCertificate { get; init; }

    /// <summary>The consumer group. A unique group replays every raw record from the beginning (drain); a stable group resumes.</summary>
    public string ConsumerGroup { get; init; } = "dfs-curation";

    /// <summary>Replication factor for auto-created curated topics (brokers have auto-create off).</summary>
    public short ReplicationFactor { get; init; } = 3;

    /// <summary>In drain mode, stop once no raw record arrives for this long (the snapshot is fully consumed).</summary>
    public TimeSpan DrainIdleTimeout { get; init; } = TimeSpan.FromSeconds(15);
}
