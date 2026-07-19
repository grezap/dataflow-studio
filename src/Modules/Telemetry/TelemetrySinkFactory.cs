using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Builds a telemetry sink for the runnable consoles (curation / warehouse-sink drains), which
/// construct their engines directly rather than through DI. Returns a live
/// <see cref="KafkaTelemetrySink"/> (topics ensured) when Kafka is configured, otherwise the no-op
/// <see cref="NullPipelineTelemetrySink"/> so an unconfigured run stays clean. The caller flushes +
/// disposes (the returned sink may implement <see cref="IAsyncDisposable"/>).
/// </summary>
public static class TelemetrySinkFactory
{
    /// <summary>
    /// Creates a telemetry sink from configuration, ensuring the <c>dfs.telemetry.*</c> topics exist
    /// when it is the live Kafka sink.
    /// </summary>
    /// <param name="configuration">The host configuration (environment variables).</param>
    /// <param name="loggerFactory">Logger factory for the sink components.</param>
    /// <param name="cancellationToken">Cancels topic creation.</param>
    public static async Task<IPipelineTelemetrySink> CreateAsync(
        IConfiguration configuration,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        if (!TelemetryOptionsFactory.TryFromConfiguration(configuration, out var options))
        {
            return NullPipelineTelemetrySink.Instance;
        }

        var errorSink = new ClickHouseErrorSink(options, loggerFactory.CreateLogger<ClickHouseErrorSink>());
        var sink = new KafkaTelemetrySink(options, loggerFactory.CreateLogger<KafkaTelemetrySink>(), errorSink);
        await sink.EnsureTopicsAsync(cancellationToken).ConfigureAwait(false);
        return sink;
    }
}
