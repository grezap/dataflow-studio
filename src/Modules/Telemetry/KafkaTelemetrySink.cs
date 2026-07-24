using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Logging;
using Nexus.Kafka;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// The native-ingestion telemetry sink: it serializes each telemetry record to a single JSON line
/// (<c>JSONEachRow</c>) and produces it to the <c>dfs.telemetry.*</c> Kafka topics, from which
/// ClickHouse's Kafka-engine tables ingest natively (ADR-0008). Timestamps are emitted as epoch
/// milliseconds (<c>event_ms</c>) so the ClickHouse materialized views convert them unambiguously with
/// <c>fromUnixTimestamp64Milli</c> — no locale-sensitive datetime-string parsing on the broker path.
/// Errors normally flow natively too; when the broker is unreachable they fall back to the direct
/// ClickHouse HTTPS inserter (<see cref="ClickHouseErrorSink"/>). An OpenTelemetry counter records
/// every emit (E16) — inert until an OTLP exporter is wired (the observability tier is off this week).
/// </summary>
/// <param name="options">Kafka connection + telemetry settings.</param>
/// <param name="logger">Diagnostics log.</param>
/// <param name="errorFallback">The direct-HTTPS error inserter used when the native path fails.</param>
public sealed partial class KafkaTelemetrySink(
    TelemetryOptions options,
    ILogger<KafkaTelemetrySink> logger,
    ClickHouseErrorSink errorFallback) : IPipelineTelemetrySink, IAsyncDisposable
{
    /// <summary>The Meter name the OTLP exporter registers (via the additional-meters list) so the emit counter exports (E16).</summary>
    public const string MeterName = "DataFlowStudio.Telemetry";

    private static readonly Meter Meter = new(MeterName);
    private static readonly Counter<long> EmittedCounter =
        Meter.CreateCounter<long>("dfs.telemetry.emitted", unit: "records", description: "Telemetry records produced, by stream.");

    private readonly IProducer<Null, string> _producer =
        new ProducerBuilder<Null, string>(KafkaClientFactory.CreateProducerConfig(options.Kafka)).Build();
    private readonly ConcurrentBag<Task> _pendingFallbacks = [];

    /// <summary>Creates the three <c>dfs.telemetry.*</c> topics (brokers have auto-create off). Idempotent.</summary>
    /// <param name="cancellationToken">Cancels the operation.</param>
    public async Task EnsureTopicsAsync(CancellationToken cancellationToken = default)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = options.Kafka.BootstrapServers,
            SecurityProtocol = SecurityProtocol.Ssl,
            SslCaPem = options.Kafka.CaCertPem,
            SslCertificatePem = options.Kafka.ClientCertPem,
            SslKeyPem = options.Kafka.ClientKeyPem,
        };
        using var admin = new AdminClientBuilder(adminConfig).Build();

        var specs = TelemetryTopics.All
            .Select(name => new TopicSpecification
            {
                Name = name,
                NumPartitions = 1,
                ReplicationFactor = options.ReplicationFactor,
            })
            .ToList();

        try
        {
            await admin.CreateTopicsAsync(specs).ConfigureAwait(false);
            LogTopicsEnsured(logger);
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            // Some or all telemetry topics already existed — fine.
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <inheritdoc />
    public void RecordStage(PipelineStageEvent stageEvent)
    {
        ArgumentNullException.ThrowIfNull(stageEvent);
        ProduceFireAndForget(TelemetryTopics.PipelineEvents, TelemetryWire.PipelineEventJson(stageEvent), "pipeline_events");
    }

    /// <inheritdoc />
    public void RecordCdcLag(CdcLagSample sample)
    {
        ArgumentNullException.ThrowIfNull(sample);
        ProduceFireAndForget(TelemetryTopics.CdcLag, TelemetryWire.CdcLagJson(sample), "cdc_lag");
    }

    /// <inheritdoc />
    public void RecordError(PipelineError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        var json = TelemetryWire.ErrorEventJson(error);

        try
        {
            // Deliver natively; if the broker rejects delivery, fall back to the direct HTTPS control path.
            _producer.Produce(
                TelemetryTopics.ErrorEvents,
                new Message<Null, string> { Value = json },
                report =>
                {
                    if (report.Error.IsError)
                    {
                        _pendingFallbacks.Add(errorFallback.InsertAsync(error));
                    }
                });
            EmittedCounter.Add(1, new KeyValuePair<string, object?>("stream", "error_events"));
        }
        catch (ProduceException<Null, string> ex)
        {
            // The broker is unreachable (can't even enqueue) — the error is likely ABOUT Kafka. Use HTTPS.
            LogNativeProduceFailed(logger, ex.Error.Reason);
            _pendingFallbacks.Add(errorFallback.InsertAsync(error));
        }
    }

    /// <inheritdoc />
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        // Drain librdkafka's queue first (runs delivery handlers, which may enqueue HTTPS fallbacks)…
        _producer.Flush(cancellationToken);
        // …then await any fallback inserts those handlers scheduled.
        await Task.WhenAll(_pendingFallbacks).ConfigureAwait(false);
    }

    private void ProduceFireAndForget(string topic, string json, string stream)
    {
        try
        {
            _producer.Produce(topic, new Message<Null, string> { Value = json });
            EmittedCounter.Add(1, new KeyValuePair<string, object?>("stream", stream));
        }
        catch (ProduceException<Null, string> ex)
        {
            // Non-error telemetry is best-effort: a produce failure must never disrupt the pipeline.
            LogNativeProduceFailed(logger, ex.Error.Reason);
        }
    }

    /// <summary>Flushes and disposes the producer.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogNativeProduceFailed(logger, ex.Message);
        }

        _producer.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Telemetry topics ensured (dfs.telemetry.*).")]
    private static partial void LogTopicsEnsured(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native telemetry produce failed: {Reason}")]
    private static partial void LogNativeProduceFailed(ILogger logger, string reason);
}
