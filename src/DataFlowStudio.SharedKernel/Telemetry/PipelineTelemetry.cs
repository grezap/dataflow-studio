namespace DataFlowStudio.SharedKernel.Telemetry;

/// <summary>
/// A per-stage pipeline telemetry record — one row in ClickHouse <c>analytics.pipeline_events</c>.
/// The pipeline stages (curation, warehouse-sink) emit one of these per unit of work so latency and
/// throughput are observable end-to-end. The shape mirrors the ClickHouse columns exactly (ADR-0008).
/// </summary>
/// <param name="EventTime">When the stage completed (UTC).</param>
/// <param name="TraceId">Correlation id tying this stage to a run / OpenTelemetry trace.</param>
/// <param name="Pipeline">The pipeline the stage belongs to (e.g. <c>curation</c>, <c>warehouse-sink</c>).</param>
/// <param name="Stage">The stage within the pipeline (e.g. <c>drain</c>, <c>dim_customer</c>, <c>fact_order</c>).</param>
/// <param name="Status">Terminal status of the stage (<c>ok</c> / <c>error</c>).</param>
/// <param name="DurationMs">Wall-clock duration of the stage in milliseconds.</param>
/// <param name="Payload">A small JSON payload with stage-specific detail (e.g. record counts).</param>
public sealed record PipelineStageEvent(
    DateTimeOffset EventTime,
    string TraceId,
    string Pipeline,
    string Stage,
    string Status,
    uint DurationMs,
    string Payload);

/// <summary>
/// A single end-to-end CDC-lag sample — one row in ClickHouse <c>analytics.cdc_lag_seconds</c>.
/// Measured as <c>now − source-commit-time</c> when a change is curated, it quantifies how fresh the
/// pipeline's view of the source is (the freshness SLO).
/// </summary>
/// <param name="EventTime">When the sample was taken (UTC).</param>
/// <param name="Source">The logical source of the change (e.g. <c>oltp</c>).</param>
/// <param name="Topic">The raw CDC topic the change arrived on.</param>
/// <param name="LagSeconds">Seconds between the source commit and processing (may be fractional).</param>
public sealed record CdcLagSample(
    DateTimeOffset EventTime,
    string Source,
    string Topic,
    double LagSeconds);

/// <summary>
/// A structured pipeline failure — one row in ClickHouse <c>analytics.error_events</c>. Emitted when a
/// stage skips a bad record or aborts, so failures are queryable and alertable (trace-correlated).
/// </summary>
/// <param name="EventTime">When the error occurred (UTC).</param>
/// <param name="TraceId">Correlation id tying this error to a run / trace.</param>
/// <param name="Service">The service/stage that raised it (e.g. <c>curation</c>).</param>
/// <param name="ErrorCode">A short, stable classification (e.g. <c>projection-failed</c>).</param>
/// <param name="Message">The human-readable error message.</param>
/// <param name="Stack">The exception stack (or empty when there is none).</param>
public sealed record PipelineError(
    DateTimeOffset EventTime,
    string TraceId,
    string Service,
    string ErrorCode,
    string Message,
    string Stack);

/// <summary>
/// The pipeline's self-observation contract. Stages (curation, warehouse-sink) depend on this
/// abstraction — defined in the SharedKernel so a module never references the Telemetry module (module
/// isolation, ADR-0001) — and the Telemetry module supplies the concrete Kafka/ClickHouse sink. The
/// record methods are cheap, fire-and-forget enqueues (they must not block the hot path); durable
/// delivery is completed by <see cref="FlushAsync"/> at the end of a run.
/// </summary>
public interface IPipelineTelemetrySink
{
    /// <summary>Records a completed pipeline stage (latency + status).</summary>
    /// <param name="stageEvent">The stage telemetry.</param>
    void RecordStage(PipelineStageEvent stageEvent);

    /// <summary>Records a single CDC-lag sample (source freshness).</summary>
    /// <param name="sample">The lag sample.</param>
    void RecordCdcLag(CdcLagSample sample);

    /// <summary>Records a structured pipeline error.</summary>
    /// <param name="error">The error telemetry.</param>
    void RecordError(PipelineError error);

    /// <summary>Flushes any buffered telemetry to its destination(s). Call once a run completes.</summary>
    /// <param name="cancellationToken">Cancels the flush.</param>
    Task FlushAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// The no-op telemetry sink. Engines default to this so a run that is not wired for telemetry (no
/// Kafka/ClickHouse configured) still executes cleanly — instrumentation calls simply do nothing.
/// </summary>
public sealed class NullPipelineTelemetrySink : IPipelineTelemetrySink
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly NullPipelineTelemetrySink Instance = new();

    private NullPipelineTelemetrySink()
    {
    }

    /// <inheritdoc />
    public void RecordStage(PipelineStageEvent stageEvent)
    {
        // no-op
    }

    /// <inheritdoc />
    public void RecordCdcLag(CdcLagSample sample)
    {
        // no-op
    }

    /// <inheritdoc />
    public void RecordError(PipelineError error)
    {
        // no-op
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}
