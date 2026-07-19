using System.Text.Json;
using System.Text.Json.Serialization;
using DataFlowStudio.SharedKernel.Telemetry;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// Serializes telemetry records to the single-line JSON (<c>JSONEachRow</c>) the ClickHouse
/// Kafka-engine source tables consume. The JSON property names must match the source-table columns
/// exactly (Script0005), and timestamps are emitted as epoch milliseconds (<c>event_ms</c>) so the
/// materialized views convert them with <c>fromUnixTimestamp64Milli</c> — no locale-sensitive datetime
/// parsing on the broker path (ADR-0008). Kept public + separate from the producer so the wire shape is
/// unit-testable against the DDL.
/// </summary>
public static class TelemetryWire
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    /// <summary>The JSON line for a <c>dfs.telemetry.pipeline_events</c> record.</summary>
    /// <param name="stageEvent">The stage event.</param>
    public static string PipelineEventJson(PipelineStageEvent stageEvent)
    {
        ArgumentNullException.ThrowIfNull(stageEvent);
        return JsonSerializer.Serialize(
            new PipelineEventRow(
                stageEvent.EventTime.ToUnixTimeMilliseconds(), stageEvent.TraceId, stageEvent.Pipeline,
                stageEvent.Stage, stageEvent.Status, stageEvent.DurationMs, stageEvent.Payload),
            Options);
    }

    /// <summary>The JSON line for a <c>dfs.telemetry.cdc_lag</c> record.</summary>
    /// <param name="sample">The CDC-lag sample.</param>
    public static string CdcLagJson(CdcLagSample sample)
    {
        ArgumentNullException.ThrowIfNull(sample);
        return JsonSerializer.Serialize(
            new CdcLagRow(sample.EventTime.ToUnixTimeMilliseconds(), sample.Source, sample.Topic, sample.LagSeconds),
            Options);
    }

    /// <summary>The JSON line for a <c>dfs.telemetry.error_events</c> record.</summary>
    /// <param name="error">The error.</param>
    public static string ErrorEventJson(PipelineError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return JsonSerializer.Serialize(
            new ErrorEventRow(
                error.EventTime.ToUnixTimeMilliseconds(), error.TraceId, error.Service,
                error.ErrorCode, error.Message, error.Stack),
            Options);
    }

    private sealed record PipelineEventRow(
        [property: JsonPropertyName("event_ms")] long EventMs,
        [property: JsonPropertyName("trace_id")] string TraceId,
        [property: JsonPropertyName("pipeline")] string Pipeline,
        [property: JsonPropertyName("stage")] string Stage,
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("duration_ms")] uint DurationMs,
        [property: JsonPropertyName("payload")] string Payload);

    private sealed record CdcLagRow(
        [property: JsonPropertyName("event_ms")] long EventMs,
        [property: JsonPropertyName("source")] string Source,
        [property: JsonPropertyName("topic")] string Topic,
        [property: JsonPropertyName("lag_seconds")] double LagSeconds);

    private sealed record ErrorEventRow(
        [property: JsonPropertyName("event_ms")] long EventMs,
        [property: JsonPropertyName("trace_id")] string TraceId,
        [property: JsonPropertyName("service")] string Service,
        [property: JsonPropertyName("error_code")] string ErrorCode,
        [property: JsonPropertyName("message")] string Message,
        [property: JsonPropertyName("stack")] string Stack);
}
