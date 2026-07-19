using Microsoft.Extensions.Configuration;
using Nexus.Kafka;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Builds <see cref="TelemetryOptions"/> from configuration (environment variables in every host). The
/// Kafka mTLS material is referenced by file path so secrets stay on disk (issued from Vault per run).
/// Returns false when the Kafka connection is not configured, so a host boots (and the pipeline runs)
/// without telemetry wired. The ClickHouse error path + OTLP endpoint are optional add-ons.
/// </summary>
public static class TelemetryOptionsFactory
{
    /// <summary>
    /// Attempts to build options. Requires <c>DFS_KAFKA_BOOTSTRAP</c> and readable
    /// <c>DFS_KAFKA_CA</c>/<c>DFS_KAFKA_CERT</c>/<c>DFS_KAFKA_KEY</c> PEM files. Optional:
    /// <c>DFS_CLICKHOUSE_CONNECTION</c> (+ <c>DFS_CLICKHOUSE_CACERT</c> / <c>DFS_CLICKHOUSE_CLIENT_PFX</c>
    /// [<c>_PASSWORD</c>]) for the direct-HTTPS error path, and <c>DFS_OTLP_ENDPOINT</c> for OTel export.
    /// </summary>
    public static bool TryFromConfiguration(IConfiguration configuration, out TelemetryOptions options)
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

        var otlp = configuration["DFS_OTLP_ENDPOINT"];

        options = new TelemetryOptions
        {
            Kafka = new KafkaConnectionOptions
            {
                BootstrapServers = bootstrap,
                CaCertPem = File.ReadAllText(caPath),
                ClientCertPem = File.ReadAllText(certPath),
                ClientKeyPem = File.ReadAllText(keyPath),
            },
            ClickHouseConnectionString = configuration["DFS_CLICKHOUSE_CONNECTION"],
            ClickHouseCaCertPath = configuration["DFS_CLICKHOUSE_CACERT"],
            ClickHouseClientPfxPath = configuration["DFS_CLICKHOUSE_CLIENT_PFX"],
            ClickHouseClientPfxPassword = configuration["DFS_CLICKHOUSE_CLIENT_PFX_PASSWORD"],
            ServiceName = configuration["DFS_OTEL_SERVICE"] ?? "dataflow-studio",
            OtlpEndpoint = string.IsNullOrWhiteSpace(otlp) ? null : new Uri(otlp),
        };
        return true;
    }
}
