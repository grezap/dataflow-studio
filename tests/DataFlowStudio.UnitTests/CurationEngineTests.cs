using Avro.Generic;
using Confluent.Kafka;
using DataFlowStudio.Modules.Ingestion.Curation;
using DataFlowStudio.SharedKernel.Lineage;
using DataFlowStudio.UnitTests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.Kafka;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The curation engine's per-record path (<c>TryCurateAsync</c>, exposed internally so it is testable
/// against a recording producer): a raw Debezium change is parsed, projected to a curated Avro record,
/// produced to the entity's curated topic, and instrumented (stage event + CDC-lag). A bad message is
/// skipped with a structured error — it never crashes the drain (ADR-0008).
/// </summary>
public sealed class CurationEngineTests
{
    private static readonly EntityCurationSpec Customers = CurationCatalog.All.Single(s => s.Entity == "customers");

    private const string CustomerAfter =
        """{"CustomerId":42,"CustomerCode":"SEED-C001","DisplayName":"Ada Lovelace","Email":"ada@example.com","PreferredLocale":"en-US","Status":1,"LifetimeValueUsd":"318.18"}""";

    private static string Envelope(string op, long tsMs, string afterJson) =>
        "{\"payload\":{\"op\":\"" + op + "\",\"source\":{\"ts_ms\":" + tsMs + "},\"after\":" + afterJson + "}}";

    private static CurationEngine Engine(RecordingTelemetrySink telemetry) =>
        new(
            new CurationOptions
            {
                Kafka = new KafkaConnectionOptions
                {
                    BootstrapServers = "unused:9092",
                    CaCertPem = "ca",
                    ClientCertPem = "cert",
                    ClientKeyPem = "key",
                },
                SchemaRegistryUrl = "https://unused",
            },
            NullLogger<CurationEngine>.Instance,
            telemetry,
            NullLineageEmitter.Instance);

    private static ConsumeResult<string, string> Raw(string topic, string value) =>
        new() { Topic = topic, Message = new Message<string, string> { Key = "k", Value = value } };

    private static Dictionary<string, int> Counts() =>
        CurationCatalog.All.ToDictionary(s => s.Entity, _ => 0, StringComparer.Ordinal);

    [Fact]
    public async Task Curates_a_change_produces_curated_record_and_records_telemetry()
    {
        var telemetry = new RecordingTelemetrySink();
        var producer = new FakeProducer<string, GenericRecord>();
        var counts = Counts();

        var ok = await Engine(telemetry).TryCurateAsync(
            producer, Raw(Customers.RawTopic, Envelope("c", 1_700_000_000_000, CustomerAfter)), counts, "trace-x", CancellationToken.None);

        ok.ShouldBeTrue();
        var produced = producer.Produced.ShouldHaveSingleItem();
        produced.Topic.ShouldBe(Customers.CuratedTopic);
        produced.Key.ShouldBe("SEED-C001");
        counts["customers"].ShouldBe(1);

        var stage = telemetry.Stages.ShouldHaveSingleItem();
        stage.Pipeline.ShouldBe("curation");
        stage.Stage.ShouldBe("customers");
        stage.Status.ShouldBe("ok");
        telemetry.Lags.ShouldHaveSingleItem().Topic.ShouldBe(Customers.RawTopic);   // ts_ms > 0 → a lag sample
        telemetry.Errors.ShouldBeEmpty();
    }

    [Fact]
    public async Task Unknown_topic_is_ignored()
    {
        var telemetry = new RecordingTelemetrySink();
        var producer = new FakeProducer<string, GenericRecord>();

        var ok = await Engine(telemetry).TryCurateAsync(
            producer, Raw("oltp.OltpDb.dbo.NotACatalogTable", Envelope("c", 1, CustomerAfter)), Counts(), "t", CancellationToken.None);

        ok.ShouldBeFalse();
        producer.Produced.ShouldBeEmpty();
        telemetry.Errors.ShouldBeEmpty();
    }

    [Fact]
    public async Task Unparseable_message_is_skipped_with_a_parse_error()
    {
        var telemetry = new RecordingTelemetrySink();
        var producer = new FakeProducer<string, GenericRecord>();

        var ok = await Engine(telemetry).TryCurateAsync(
            producer, Raw(Customers.RawTopic, "{ this is not valid json"), Counts(), "t", CancellationToken.None);

        ok.ShouldBeFalse();
        producer.Produced.ShouldBeEmpty();
        telemetry.Errors.ShouldHaveSingleItem().ErrorCode.ShouldBe("parse-failed");
    }

    [Fact]
    public async Task Unprojectable_record_is_skipped_with_a_projection_error()
    {
        var telemetry = new RecordingTelemetrySink();
        var producer = new FakeProducer<string, GenericRecord>();

        // A customers change missing non-nullable columns → the projector throws → skip + projection-failed.
        var ok = await Engine(telemetry).TryCurateAsync(
            producer, Raw(Customers.RawTopic, Envelope("c", 1, """{"CustomerId":1,"CustomerCode":"X"}""")), Counts(), "t", CancellationToken.None);

        ok.ShouldBeFalse();
        producer.Produced.ShouldBeEmpty();
        telemetry.Errors.ShouldHaveSingleItem().ErrorCode.ShouldBe("projection-failed");
    }

    [Fact]
    public async Task No_cdc_lag_sample_when_source_timestamp_is_absent()
    {
        var telemetry = new RecordingTelemetrySink();
        var producer = new FakeProducer<string, GenericRecord>();

        await Engine(telemetry).TryCurateAsync(
            producer, Raw(Customers.RawTopic, Envelope("c", 0, CustomerAfter)), Counts(), "t", CancellationToken.None);

        telemetry.Stages.ShouldHaveSingleItem();   // still curated + a stage event…
        telemetry.Lags.ShouldBeEmpty();             // …but ts_ms == 0 → no lag sample
    }
}
