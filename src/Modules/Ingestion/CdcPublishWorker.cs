using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Modules.Ingestion;

/// <summary>
/// Background worker that pumps CDC changes from OltpDb to Kafka (Avro). Week-1 slice is a
/// heartbeat skeleton; the Week-2 slice wires the SQL Server CDC capture-instance poll (Dapper),
/// the Avro serialization + Schema Registry registration (<c>Nexus.Avro</c> / <c>Nexus.Kafka</c>
/// from nexus-shared), and the OpenLineage run events (E16). Kept strictly AOT/trim-safe.
/// </summary>
public sealed partial class CdcPublishWorker(ILogger<CdcPublishWorker> logger) : BackgroundService
{
    // How often the worker polls SQL Server's CDC capture instances. A fixed cadence keeps CDC lag
    // bounded and predictable; it becomes configurable when the real poll lands in Week 2.
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The worker loop. Runs until the host signals shutdown via <paramref name="stoppingToken"/>.
    /// Week-1 slice is a heartbeat; Week 2 replaces the loop body with: poll
    /// <c>cdc.fn_cdc_get_all_changes_*</c> (Dapper) → map to Avro → produce to Kafka → advance the
    /// committed offset. A <see cref="PeriodicTimer"/> is used rather than <c>Task.Delay</c> so ticks
    /// do not drift and cancellation is observed promptly.
    /// </summary>
    /// <param name="stoppingToken">Cancels the loop when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarting(logger);

        using var timer = new PeriodicTimer(PollInterval);

        // WaitForNextTickAsync returns false when the token is cancelled, which ends the loop
        // cleanly (no OperationCanceledException to swallow).
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            // Week 2: poll cdc.fn_cdc_get_all_changes_* → map to Avro → produce to Kafka.
            LogHeartbeat(logger);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "CDC publish worker started (poll skeleton).")]
    private static partial void LogStarting(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CDC publish worker tick — no CDC source wired yet (Week 2).")]
    private static partial void LogHeartbeat(ILogger logger);
}
