using Microsoft.Extensions.Configuration;
using Nexus.Observability;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Starts OpenTelemetry export for a runnable console (curation / warehouse-sink drains), which build
/// their engines directly rather than through DI (E16). It owns a standalone
/// <see cref="TracerProvider"/> + <see cref="MeterProvider"/> so the pipeline's spans and the emit
/// counter reach the lab collector. Start it <em>before</em> the traced work runs and dispose it after
/// (the drain completes): disposal shuts the providers down and flushes the final batch of spans and
/// the last metric collection. When <c>DFS_OTLP_ENDPOINT</c> is unset, <see cref="TryStart"/> returns
/// <see langword="null"/> and the run proceeds with no export overhead.
/// </summary>
public sealed class ObservabilityConsole : IDisposable
{
    private readonly TracerProvider _tracerProvider;
    private readonly MeterProvider _meterProvider;

    private ObservabilityConsole(TracerProvider tracerProvider, MeterProvider meterProvider)
    {
        _tracerProvider = tracerProvider;
        _meterProvider = meterProvider;
    }

    /// <summary>
    /// Builds and starts the tracer + meter providers when OTLP export is configured; returns null otherwise.
    /// </summary>
    /// <param name="configuration">The host configuration (environment variables).</param>
    /// <param name="serviceName">The OTel service name stamped on the resource.</param>
    public static ObservabilityConsole? TryStart(IConfiguration configuration, string serviceName)
    {
        if (!ObservabilityWiring.TryCreateOptions(configuration, serviceName, out var options))
        {
            return null;
        }

        return new ObservabilityConsole(
            ObservabilityBootstrap.BuildTracerProvider(options),
            ObservabilityBootstrap.BuildMeterProvider(options));
    }

    /// <summary>Shuts the providers down, flushing any buffered spans and the final metric collection.</summary>
    public void Dispose()
    {
        _tracerProvider.Dispose();
        _meterProvider.Dispose();
    }
}
