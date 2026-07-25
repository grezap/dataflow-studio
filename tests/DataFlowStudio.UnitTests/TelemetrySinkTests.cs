using Confluent.Kafka;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.SharedKernel.Telemetry;
using DataFlowStudio.UnitTests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.Kafka;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The native Kafka telemetry sink (ADR-0008) against a recording producer + fallback: stage/lag events
/// produce to their <c>dfs.telemetry.*</c> topics, and errors take the dual path — normally native, but
/// a delivery failure OR an un-enqueueable produce (broker unreachable) falls back to the direct-HTTPS
/// inserter. Plus the ClickHouse error inserter's enabled/guard behaviour.
/// </summary>
public sealed class TelemetrySinkTests
{
    private static TelemetryOptions Options(string? clickHouse = null) => new()
    {
        Kafka = new KafkaConnectionOptions
        {
            BootstrapServers = "unused:9092",
            CaCertPem = "ca",
            ClientCertPem = "cert",
            ClientKeyPem = "key",
        },
        ClickHouseConnectionString = clickHouse,
    };

    private static PipelineError Error() =>
        new(DateTimeOffset.UnixEpoch, "trace-1", "curation", "boom", "it broke", string.Empty);

    private static KafkaTelemetrySink Sink(
        FakeProducer<Null, string> producer, RecordingErrorFallbackSink fallback) =>
        new(Options(), NullLogger<KafkaTelemetrySink>.Instance, fallback, producer);

    [Fact]
    public void RecordStage_produces_to_the_pipeline_events_topic()
    {
        var producer = new FakeProducer<Null, string>();
        var sink = Sink(producer, new RecordingErrorFallbackSink());

        sink.RecordStage(new PipelineStageEvent(DateTimeOffset.UnixEpoch, "t", "curation", "customers", "ok", 3, "{}"));

        var produced = producer.Produced.ShouldHaveSingleItem();
        produced.Topic.ShouldBe("dfs.telemetry.pipeline_events");
        produced.Value.ShouldContain("customers");
    }

    [Fact]
    public void RecordCdcLag_produces_to_the_cdc_lag_topic()
    {
        var producer = new FakeProducer<Null, string>();
        var sink = Sink(producer, new RecordingErrorFallbackSink());

        sink.RecordCdcLag(new CdcLagSample(DateTimeOffset.UnixEpoch, "oltp", "oltp.raw.orders", 12.5));

        producer.Produced.ShouldHaveSingleItem().Topic.ShouldBe("dfs.telemetry.cdc_lag");
    }

    [Fact]
    public async Task RecordError_delivers_natively_when_the_broker_is_healthy()
    {
        var producer = new FakeProducer<Null, string>();
        var fallback = new RecordingErrorFallbackSink();
        var sink = Sink(producer, fallback);

        sink.RecordError(Error());
        await sink.FlushAsync();

        producer.Produced.ShouldHaveSingleItem().Topic.ShouldBe("dfs.telemetry.error_events");
        fallback.Inserted.ShouldBeEmpty();   // native delivery succeeded → no fallback
    }

    [Fact]
    public async Task RecordError_falls_back_when_delivery_reports_an_error()
    {
        var producer = new FakeProducer<Null, string> { Behavior = ProduceBehavior.DeliveryError };
        var fallback = new RecordingErrorFallbackSink();
        var sink = Sink(producer, fallback);

        sink.RecordError(Error());
        await sink.FlushAsync();

        fallback.Inserted.ShouldHaveSingleItem().ErrorCode.ShouldBe("boom");
    }

    [Fact]
    public async Task RecordError_falls_back_when_the_broker_is_unreachable()
    {
        var producer = new FakeProducer<Null, string> { Behavior = ProduceBehavior.ThrowProduceException };
        var fallback = new RecordingErrorFallbackSink();
        var sink = Sink(producer, fallback);

        sink.RecordError(Error());   // produce throws → HTTPS fallback (error is likely ABOUT Kafka)
        await sink.FlushAsync();

        fallback.Inserted.ShouldHaveSingleItem();
        producer.Flushes.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void RecordStage_produce_failure_is_swallowed_best_effort()
    {
        // Non-error telemetry is fire-and-forget: a produce failure must never disrupt the pipeline.
        var producer = new FakeProducer<Null, string> { Behavior = ProduceBehavior.ThrowProduceException };
        var sink = Sink(producer, new RecordingErrorFallbackSink());

        Should.NotThrow(() => sink.RecordStage(new PipelineStageEvent(DateTimeOffset.UnixEpoch, "t", "curation", "s", "ok", 1, "{}")));
    }

    [Fact]
    public async Task DisposeAsync_flushes_and_disposes_the_producer()
    {
        var producer = new FakeProducer<Null, string>();
        var sink = Sink(producer, new RecordingErrorFallbackSink());

        await sink.DisposeAsync();

        producer.Flushes.ShouldBeGreaterThan(0);
        producer.Disposed.ShouldBeTrue();
    }

    [Fact]
    public void ClickHouseErrorSink_is_disabled_without_a_connection_string()
    {
        var sink = new ClickHouseErrorSink(Options(clickHouse: null), NullLogger<ClickHouseErrorSink>.Instance);
        sink.IsEnabled.ShouldBeFalse();
    }

    [Fact]
    public async Task ClickHouseErrorSink_disabled_insert_is_a_no_op()
    {
        var sink = new ClickHouseErrorSink(Options(clickHouse: null), NullLogger<ClickHouseErrorSink>.Instance);
        await Should.NotThrowAsync(() => sink.InsertAsync(Error()));
    }

    [Fact]
    public async Task ClickHouseErrorSink_enabled_insert_swallows_a_connection_failure()
    {
        // Enabled (a connection string is set) but the server is unreachable → the insert attempt fails
        // and is swallowed; telemetry must never crash the pipeline.
        var sink = new ClickHouseErrorSink(
            Options(clickHouse: "Host=127.0.0.1;Port=1;Database=analytics;User=x;Password=y"),
            NullLogger<ClickHouseErrorSink>.Instance);

        sink.IsEnabled.ShouldBeTrue();
        await Should.NotThrowAsync(() => sink.InsertAsync(Error()));
    }
}
