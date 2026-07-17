using DataFlowStudio.Modules.Warehouse.Sink;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Modules.Warehouse;

/// <summary>
/// The hosted StarRocks DWH sink worker. Periodically runs the <see cref="WarehouseSinkEngine"/> to
/// load the current curated snapshot into the Kimball star (SCD2 dimensions + facts). The runnable
/// <c>DataFlowStudio.WarehouseSink</c> console drives the same engine once (drain) for demos and the
/// live load, so there is a single load code path.
/// </summary>
public sealed partial class WarehouseSinkWorker(WarehouseSinkEngine engine, ILogger<WarehouseSinkWorker> logger) : BackgroundService
{
    private static readonly TimeSpan LoadInterval = TimeSpan.FromMinutes(1);

    /// <summary>Runs a load cycle on each tick until the host signals shutdown.</summary>
    /// <param name="stoppingToken">Cancels the loop on host shutdown.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarting(logger);
        using var timer = new PeriodicTimer(LoadInterval);
        do
        {
            try
            {
                await engine.RunAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "StarRocks DWH sink worker starting.")]
    private static partial void LogStarting(ILogger logger);
}
