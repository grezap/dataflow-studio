using ClickHouse.Client.Utility;
using DataFlowStudio.Clickhouse;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Inserts structured pipeline errors directly into ClickHouse <c>analytics.error_events</c> over the
/// HTTPS interface (via the shared <see cref="TlsClickHouseConnectionFactory"/> that trusts the lab's
/// private Vault-PKI root). This is the synchronous <em>control</em> path for errors, and the
/// <em>fallback</em> when the native Kafka path is unavailable — e.g. an error <em>about</em> Kafka,
/// where the worker cannot produce to a broker it can't reach (ADR-0008). Inert (a logged no-op) when
/// no ClickHouse connection is configured, so a Kafka-only run still works.
/// </summary>
public sealed partial class ClickHouseErrorSink(TelemetryOptions options, ILogger<ClickHouseErrorSink> logger)
{
    /// <summary>Whether a ClickHouse connection is configured (the sink is active).</summary>
    public bool IsEnabled => !string.IsNullOrWhiteSpace(options.ClickHouseConnectionString);

    /// <summary>Inserts one error row. Swallows and logs failures — telemetry must never crash a pipeline.</summary>
    /// <param name="error">The error to record.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    public async Task InsertAsync(PipelineError error, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (!IsEnabled)
        {
            return;
        }

        try
        {
            await using var connection = TlsClickHouseConnectionFactory.Create(
                options.ClickHouseConnectionString!,
                options.ClickHouseCaCertPath,
                options.ClickHouseClientPfxPath,
                options.ClickHouseClientPfxPassword);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO analytics.error_events
                    (event_time, trace_id, service, error_code, message, stack)
                VALUES
                    ({event_time:DateTime64(3)}, {trace_id:String}, {service:String},
                     {error_code:String}, {message:String}, {stack:String})
                """;
            command.AddParameter("event_time", "DateTime64(3)", error.EventTime.UtcDateTime);
            command.AddParameter("trace_id", "String", error.TraceId);
            command.AddParameter("service", "String", error.Service);
            command.AddParameter("error_code", "String", error.ErrorCode);
            command.AddParameter("message", "String", error.Message);
            command.AddParameter("stack", "String", error.Stack);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // A telemetry failure must not take down the pipeline; log and move on.
            LogInsertFailed(logger, error.ErrorCode, ex.Message);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "ClickHouse error-insert failed ({Code}): {Reason}")]
    private static partial void LogInsertFailed(ILogger logger, string code, string reason);
}
