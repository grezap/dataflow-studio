using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Hosting;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// A hosted startup task that ensures the <c>dfs.telemetry.*</c> topics exist before the pipeline
/// workers begin producing telemetry (the brokers have auto-create off). The runnable consoles ensure
/// topics themselves via <see cref="TelemetrySinkFactory"/>; this covers the Api-hosted path.
/// </summary>
[ExcludeFromCodeCoverage] // Hosted startup task — delegates to the (excluded) Kafka admin call (ADR-0012).
public sealed class TelemetryTopicInitializer(KafkaTelemetrySink sink) : IHostedService
{
    /// <summary>Ensures the telemetry topics on host start.</summary>
    /// <param name="cancellationToken">Cancels the operation.</param>
    public Task StartAsync(CancellationToken cancellationToken) => sink.EnsureTopicsAsync(cancellationToken);

    /// <summary>No-op on stop.</summary>
    /// <param name="cancellationToken">Unused.</param>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
