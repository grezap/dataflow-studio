using Microsoft.Extensions.Configuration;
using Nexus.Kafka;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// Builds <see cref="WarehouseSinkOptions"/> from configuration (environment variables). Requires the
/// Kafka mTLS material (by file path, resolved from Vault) and the StarRocks connection string;
/// returns false when unconfigured so a host can boot without wiring the sink.
/// </summary>
public static class WarehouseSinkOptionsFactory
{
    /// <summary>
    /// Attempts to build options. Requires <c>DFS_KAFKA_BOOTSTRAP</c>, readable
    /// <c>DFS_KAFKA_CA/CERT/KEY</c>, and <c>DFS_STARROCKS_CONNECTION</c>; <c>DFS_SR_URL</c> is optional.
    /// </summary>
    public static bool TryFromConfiguration(IConfiguration configuration, out WarehouseSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        options = null!;

        var bootstrap = configuration["DFS_KAFKA_BOOTSTRAP"];
        var caPath = configuration["DFS_KAFKA_CA"];
        var certPath = configuration["DFS_KAFKA_CERT"];
        var keyPath = configuration["DFS_KAFKA_KEY"];
        var starRocks = configuration["DFS_STARROCKS_CONNECTION"];

        if (string.IsNullOrWhiteSpace(bootstrap) || string.IsNullOrWhiteSpace(starRocks)
            || !File.Exists(caPath) || !File.Exists(certPath) || !File.Exists(keyPath))
        {
            return false;
        }

        options = new WarehouseSinkOptions
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
            StarRocksConnection = starRocks,
            ConsumerGroup = configuration["DFS_WAREHOUSE_GROUP"] ?? "dfs-warehouse-sink",
        };
        return true;
    }
}
