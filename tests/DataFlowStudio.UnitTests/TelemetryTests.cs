using System.Text.Json;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.SharedKernel.Telemetry;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The telemetry wire shape must match the ClickHouse Kafka-engine source columns exactly (Script0005),
/// or JSONEachRow ingestion silently drops rows. These tests pin the property names + the epoch-ms
/// timestamp encoding, and confirm the no-op sink is inert (ADR-0008).
/// </summary>
public sealed class TelemetryTests
{
    private static readonly DateTimeOffset At = DateTimeOffset.FromUnixTimeMilliseconds(1_737_000_000_123);

    [Fact]
    public void PipelineEventJson_matches_the_kafka_source_columns()
    {
        var json = TelemetryWire.PipelineEventJson(
            new PipelineStageEvent(At, "trace-1", "curation", "dim_customer", "ok", 42, "{\"n\":1}"));

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        PropertyNames(root).ShouldBe(
            ["event_ms", "trace_id", "pipeline", "stage", "status", "duration_ms", "payload"],
            ignoreOrder: true);
        root.GetProperty("event_ms").GetInt64().ShouldBe(1_737_000_000_123);
        root.GetProperty("pipeline").GetString().ShouldBe("curation");
        root.GetProperty("stage").GetString().ShouldBe("dim_customer");
        root.GetProperty("duration_ms").GetUInt32().ShouldBe(42u);
    }

    [Fact]
    public void CdcLagJson_matches_the_kafka_source_columns()
    {
        var json = TelemetryWire.CdcLagJson(
            new CdcLagSample(At, "oltp", "oltp.OltpDb.dbo.Customers", 1.5));

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        PropertyNames(root).ShouldBe(["event_ms", "source", "topic", "lag_seconds"], ignoreOrder: true);
        root.GetProperty("event_ms").GetInt64().ShouldBe(1_737_000_000_123);
        root.GetProperty("source").GetString().ShouldBe("oltp");
        root.GetProperty("lag_seconds").GetDouble().ShouldBe(1.5);
    }

    [Fact]
    public void ErrorEventJson_matches_columns_and_escapes_the_message()
    {
        var json = TelemetryWire.ErrorEventJson(
            new PipelineError(At, "trace-2", "curation", "projection-failed", "bad 'quote'\nand newline", "at X"));

        using var doc = JsonDocument.Parse(json);   // parses ⇒ the message was validly escaped
        var root = doc.RootElement;
        PropertyNames(root).ShouldBe(
            ["event_ms", "trace_id", "service", "error_code", "message", "stack"],
            ignoreOrder: true);
        root.GetProperty("error_code").GetString().ShouldBe("projection-failed");
        root.GetProperty("message").GetString().ShouldBe("bad 'quote'\nand newline");
    }

    [Fact]
    public async Task NullSink_is_inert()
    {
        var sink = NullPipelineTelemetrySink.Instance;

        // None of these throw, and the flush completes.
        sink.RecordStage(new PipelineStageEvent(At, "t", "p", "s", "ok", 1, "{}"));
        sink.RecordCdcLag(new CdcLagSample(At, "oltp", "topic", 0.1));
        sink.RecordError(new PipelineError(At, "t", "svc", "code", "msg", string.Empty));
        await sink.FlushAsync();
    }

    private static IEnumerable<string> PropertyNames(JsonElement obj) =>
        obj.EnumerateObject().Select(p => p.Name);
}
