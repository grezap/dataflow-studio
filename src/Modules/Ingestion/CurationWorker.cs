using DataFlowStudio.Modules.Ingestion.Curation;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Modules.Ingestion;

/// <summary>
/// The hosted CDC curation worker. Runs the <see cref="CurationEngine"/> continuously for the host's
/// lifetime, consuming Debezium raw CDC and re-producing curated Avro (ADR-0004/0007). The runnable
/// <c>DataFlowStudio.Curation</c> console drives the same engine in drain mode for demos and the
/// live source-replay, so there is a single curation code path.
/// </summary>
public sealed partial class CurationWorker(CurationEngine engine, ILogger<CurationWorker> logger) : BackgroundService
{
    /// <summary>Runs the curation engine until the host signals shutdown.</summary>
    /// <param name="stoppingToken">Cancels the loop on host shutdown.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarting(logger);
        try
        {
            await engine.RunAsync(drainMode: false, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "CDC curation worker starting.")]
    private static partial void LogStarting(ILogger logger);
}
