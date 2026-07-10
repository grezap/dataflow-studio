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
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(10);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarting(logger);

        using var timer = new PeriodicTimer(PollInterval);
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
