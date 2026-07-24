using System.Security.Cryptography.X509Certificates;
using DataFlowStudio.SharedKernel.Lineage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Lineage;

/// <summary>
/// Builds an <see cref="ILineageEmitter"/> from configuration for the hosts + runnable consoles (E16).
/// Reads <c>DFS_MARQUEZ_ENDPOINT</c> (the Marquez base URL, e.g. <c>https://192.168.70.127</c>),
/// optional <c>DFS_MARQUEZ_CACERT</c> (the lab PKI root PEM used to validate the front-door leaf), and
/// optional <c>DFS_MARQUEZ_NAMESPACE</c>. Returns the no-op emitter when no endpoint is configured, so a
/// run without lineage wired still executes cleanly.
/// </summary>
public static class LineageEmitterFactory
{
    /// <summary>Attempts to build emitter options; false when <c>DFS_MARQUEZ_ENDPOINT</c> is unset.</summary>
    /// <param name="configuration">The host configuration (environment variables).</param>
    /// <param name="options">The built options when lineage is configured.</param>
    public static bool TryFromConfiguration(IConfiguration configuration, out LineageEmitterOptions options)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        options = null!;

        var endpoint = configuration["DFS_MARQUEZ_ENDPOINT"];
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return false;
        }

        X509Certificate2Collection? roots = null;
        var caPath = configuration["DFS_MARQUEZ_CACERT"];
        if (!string.IsNullOrWhiteSpace(caPath) && File.Exists(caPath))
        {
            roots = new X509Certificate2Collection();
            roots.ImportFromPemFile(caPath);
        }

        options = new LineageEmitterOptions
        {
            Endpoint = new Uri(endpoint),
            Namespace = configuration["DFS_MARQUEZ_NAMESPACE"] is { Length: > 0 } ns ? ns : "dataflow-studio",
            ServerCaCertificates = roots,
        };
        return true;
    }

    /// <summary>Creates a live <see cref="MarquezLineageEmitter"/> when configured, else the no-op emitter.</summary>
    /// <param name="configuration">The host configuration (environment variables).</param>
    /// <param name="loggerFactory">Logger factory for the emitter.</param>
    public static ILineageEmitter Create(IConfiguration configuration, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        return TryFromConfiguration(configuration, out var options)
            ? new MarquezLineageEmitter(options, loggerFactory.CreateLogger<MarquezLineageEmitter>())
            : NullLineageEmitter.Instance;
    }
}
