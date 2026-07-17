using Microsoft.Extensions.Configuration;
using Nexus.Kafka;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// Builds <see cref="CurationOptions"/> from configuration (environment variables in every host).
/// The Kafka mTLS material is referenced by file path so the secrets stay on disk (issued from Vault
/// per run) and never land in config values. Returns false when the connection is not configured, so
/// a host can boot without wiring the live worker.
/// </summary>
public static class CurationOptionsFactory
{
    /// <summary>
    /// Attempts to build options from <paramref name="configuration"/>. Requires
    /// <c>DFS_KAFKA_BOOTSTRAP</c> and readable <c>DFS_KAFKA_CA</c>/<c>DFS_KAFKA_CERT</c>/<c>DFS_KAFKA_KEY</c>
    /// PEM files; <c>DFS_SR_URL</c> and <c>DFS_CURATION_GROUP</c> are optional (lab defaults apply).
    /// </summary>
    public static bool TryFromConfiguration(IConfiguration configuration, out CurationOptions options)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        options = null!;

        var bootstrap = configuration["DFS_KAFKA_BOOTSTRAP"];
        var caPath = configuration["DFS_KAFKA_CA"];
        var certPath = configuration["DFS_KAFKA_CERT"];
        var keyPath = configuration["DFS_KAFKA_KEY"];

        if (string.IsNullOrWhiteSpace(bootstrap)
            || !File.Exists(caPath) || !File.Exists(certPath) || !File.Exists(keyPath))
        {
            return false;
        }

        options = new CurationOptions
        {
            Kafka = new KafkaConnectionOptions
            {
                BootstrapServers = bootstrap,
                CaCertPem = File.ReadAllText(caPath),
                ClientCertPem = File.ReadAllText(certPath),
                ClientKeyPem = File.ReadAllText(keyPath),
            },
            SchemaRegistryUrl = configuration["DFS_SR_URL"] ?? "https://192.168.10.91:8081",
            VerifySchemaRegistryCertificate = false,
            ConsumerGroup = configuration["DFS_CURATION_GROUP"] ?? "dfs-curation",
        };
        return true;
    }
}
