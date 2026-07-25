using DataFlowStudio.SharedKernel.Telemetry;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// The direct-HTTPS error path the native Kafka telemetry sink falls back to when a broker is
/// unreachable (an error <em>about</em> Kafka — ADR-0008). Extracting the seam lets the sink's dual-path
/// error handling be unit-tested against a recording fallback; <see cref="ClickHouseErrorSink"/> is the
/// only production implementation.
/// </summary>
public interface IErrorFallbackSink
{
    /// <summary>Inserts one error row via the control path (best-effort; must never throw into the caller).</summary>
    /// <param name="error">The error to record.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    Task InsertAsync(PipelineError error, CancellationToken cancellationToken = default);
}
