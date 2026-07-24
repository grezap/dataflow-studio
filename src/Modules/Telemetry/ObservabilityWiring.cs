using System.Security.Cryptography.X509Certificates;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Configuration;
using Nexus.Observability;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Builds <see cref="ObservabilityOptions"/> from configuration for both the Api host and the runnable
/// consoles (E16). It resolves the OTLP endpoint (<c>DFS_OTLP_ENDPOINT</c> — the collector's
/// HTTP/protobuf receiver, e.g. <c>https://192.168.70.182:4318</c>) and the optional lab-CA trust
/// (<c>DFS_OTLP_CACERT</c> — the NexusPlatform root PEM used to validate the collector's private-CA
/// server certificate; the collector is server-TLS, so no client certificate is needed). It always
/// registers the pipeline <see cref="DataflowActivity"/> source and the telemetry
/// <see cref="KafkaTelemetrySink.MeterName"/> meter, so both traces and metrics export uniformly.
/// </summary>
public static class ObservabilityWiring
{
    /// <summary>
    /// Attempts to build export options. Returns false (export disabled) when <c>DFS_OTLP_ENDPOINT</c>
    /// is unset — the pipeline then runs with the ActivitySource/Meter listener-free, at no cost.
    /// </summary>
    /// <param name="configuration">The host configuration (environment variables).</param>
    /// <param name="serviceName">The OTel service name stamped on the resource (per console/host).</param>
    /// <param name="options">The built options when export is configured.</param>
    public static bool TryCreateOptions(IConfiguration configuration, string serviceName, out ObservabilityOptions options)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        options = null!;

        var endpoint = configuration["DFS_OTLP_ENDPOINT"];
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return false;
        }

        // Trust the lab PKI root for the collector's server cert (it presents its own intermediate, so
        // the root alone completes the chain). Absent → OS trust (works only if the CA is installed).
        X509Certificate2Collection? roots = null;
        var caPath = configuration["DFS_OTLP_CACERT"];
        if (!string.IsNullOrWhiteSpace(caPath) && File.Exists(caPath))
        {
            roots = new X509Certificate2Collection();
            roots.ImportFromPemFile(caPath);
        }

        options = new ObservabilityOptions
        {
            ServiceName = serviceName,
            OtlpEndpoint = new Uri(endpoint),
            AdditionalSources = [DataflowActivity.SourceName],
            AdditionalMeters = [KafkaTelemetrySink.MeterName],
            ServerCaCertificates = roots,
        };
        return true;
    }
}
