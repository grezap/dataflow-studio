using System.Diagnostics;
using System.Text.Json;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Logging;
using Nexus.Avro;
using Nexus.Kafka;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// The curation engine: subscribes to every raw Debezium topic in the <see cref="CurationCatalog"/>,
/// reshapes each change into a clean curated Avro record (<see cref="CuratedRecordProjector"/>), and
/// re-produces it to the entity's <c>dfs.&lt;entity&gt;.changed.v1</c> topic through the Schema
/// Registry. It runs either continuously (the hosted worker) or in drain mode (read the current
/// snapshot to idle, then stop — used by the runnable curator and the live source-replay).
/// <para>
/// It instruments itself (ADR-0008): each curated record emits a <c>curation</c> stage event (its
/// project+produce latency) and a CDC-lag sample (<c>now − source-commit-time</c>); skipped records
/// emit a structured error. Telemetry flows through the injected <see cref="IPipelineTelemetrySink"/>
/// (a no-op when the pipeline isn't wired), so instrumentation never disrupts curation.
/// </para>
/// </summary>
public sealed partial class CurationEngine(
    CurationOptions options,
    ILogger<CurationEngine> logger,
    IPipelineTelemetrySink telemetry)
{
    private const string PipelineName = "curation";

    /// <summary>
    /// Ensures the curated topics exist, then curates raw changes until cancelled (or, in drain mode,
    /// until no raw record arrives for <see cref="CurationOptions.DrainIdleTimeout"/>). Returns the
    /// per-entity count of curated records produced this run.
    /// </summary>
    /// <param name="drainMode">True to stop when the raw snapshot is fully consumed; false to run until cancelled.</param>
    /// <param name="cancellationToken">Stops the loop (host shutdown).</param>
    public async Task<IReadOnlyDictionary<string, int>> RunAsync(bool drainMode, CancellationToken cancellationToken)
    {
        await EnsureCuratedTopicsAsync().ConfigureAwait(false);

        var counts = CurationCatalog.All.ToDictionary(s => s.Entity, _ => 0, StringComparer.Ordinal);

        var srOptions = new SchemaRegistryOptions
        {
            Url = options.SchemaRegistryUrl,
            EnableCertificateVerification = options.VerifySchemaRegistryCertificate,
        };
        using var registry = AvroSerdes.CreateRegistryClient(srOptions);

        var producerConfig = KafkaClientFactory.CreateProducerConfig(options.Kafka);
        using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
            .SetValueSerializer(AvroSerdes.CreateSerializer<GenericRecord>(registry))
            .Build();

        var consumerConfig = KafkaClientFactory.CreateConsumerConfig(options.Kafka, options.ConsumerGroup);
        consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(CurationCatalog.RawTopics);

        LogStarted(logger, options.ConsumerGroup, drainMode);
        // Root span for the whole drain; the per-record curate spans nest under it, and the ClickHouse
        // pipeline_events reuse its trace id so a run correlates across Tempo + ClickHouse (E16). When no
        // OTLP exporter is wired StartActivity returns null and we fall back to a random trace id.
        using var runActivity = DataflowActivity.Source.StartActivity("curation.drain", ActivityKind.Internal);
        var traceId = runActivity?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        var runStopwatch = Stopwatch.StartNew();
        var lastMessageUtc = DateTime.UtcNow;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(2));
                }
                catch (ConsumeException ex)
                {
                    LogConsumeError(logger, ex.Error.Reason);
                    continue;
                }

                if (result is null)
                {
                    if (drainMode && DateTime.UtcNow - lastMessageUtc > options.DrainIdleTimeout)
                    {
                        break;
                    }

                    continue;
                }

                lastMessageUtc = DateTime.UtcNow;
                if (await TryCurateAsync(producer, result, counts, traceId, cancellationToken).ConfigureAwait(false))
                {
                    // curated; counter already advanced
                }
            }
        }
        finally
        {
            producer.Flush(TimeSpan.FromSeconds(10));
            consumer.Close();
        }

        int total = counts.Values.Sum();
        runStopwatch.Stop();
        runActivity?.SetTag("dfs.records", total);
        runActivity?.SetTag("dfs.drain_mode", drainMode);

        // A run-summary stage event (the whole drain's latency + per-entity counts) …
        telemetry.RecordStage(new PipelineStageEvent(
            DateTimeOffset.UtcNow, traceId, PipelineName, "drain", "ok",
            (uint)runStopwatch.ElapsedMilliseconds, JsonSerializer.Serialize(counts)));
        // … then flush telemetry so it lands before the caller inspects the results (None: flush even
        // when the run token is cancelled on shutdown).
        await telemetry.FlushAsync(CancellationToken.None).ConfigureAwait(false);

        LogDrained(logger, total);
        return counts;
    }

    private async Task<bool> TryCurateAsync(
        IProducer<string, GenericRecord> producer,
        ConsumeResult<string, string> result,
        Dictionary<string, int> counts,
        string traceId,
        CancellationToken cancellationToken)
    {
        var spec = CurationCatalog.ForRawTopic(result.Topic);
        if (spec is null || result.Message.Value is null)
        {
            return false;   // unknown topic or a tombstone
        }

        // One span per curated record — it nests under the drain root, so Tempo shows the per-entity
        // project+produce waterfall (E16). Null (no-op) when no OTLP exporter is wired.
        using var recordActivity = DataflowActivity.Source.StartActivity("curate", ActivityKind.Internal);
        recordActivity?.SetTag("dfs.raw_topic", result.Topic);
        recordActivity?.SetTag("dfs.entity", spec.Entity);

        DebeziumChange change;
        try
        {
            change = DebeziumChange.Parse(result.Message.Value);
        }
        catch (JsonException ex)
        {
            LogParseError(logger, result.Topic, ex.Message);
            RecordError(traceId, "parse-failed", ex);
            recordActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            return false;
        }

        if (!change.HasAfter)
        {
            return false;   // hard delete — OltpDb soft-deletes, so this does not occur; skip defensively
        }

        var stopwatch = Stopwatch.StartNew();
        Avro.Generic.GenericRecord record;
        string key;
        try
        {
            (record, key) = CuratedRecordProjector.Project(spec, change);
        }
        catch (InvalidOperationException ex)
        {
            LogProjectError(logger, result.Topic, ex.Message);   // malformed record — skip, don't crash the run
            RecordError(traceId, "projection-failed", ex);
            recordActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            return false;
        }

        await producer.ProduceAsync(
            spec.CuratedTopic,
            new Message<string, GenericRecord> { Key = key, Value = record },
            cancellationToken).ConfigureAwait(false);
        stopwatch.Stop();

        counts[spec.Entity]++;
        recordActivity?.SetTag("dfs.curated_topic", spec.CuratedTopic);
        recordActivity?.SetTag("dfs.key", key);

        // Per-record telemetry: the project+produce latency for this entity (a pipeline_events sample
        // feeding the latency MV) and the end-to-end CDC lag (now − source-commit-time).
        telemetry.RecordStage(new PipelineStageEvent(
            DateTimeOffset.UtcNow, traceId, PipelineName, spec.Entity, "ok",
            (uint)stopwatch.ElapsedMilliseconds, JsonSerializer.Serialize(new { entity = spec.Entity, key })));

        if (change.SourceTsMs > 0)
        {
            var lagSeconds = (DateTimeOffset.UtcNow - DateTimeOffset.FromUnixTimeMilliseconds(change.SourceTsMs)).TotalSeconds;
            telemetry.RecordCdcLag(new CdcLagSample(DateTimeOffset.UtcNow, "oltp", result.Topic, lagSeconds));
        }

        return true;
    }

    private void RecordError(string traceId, string errorCode, Exception ex) =>
        telemetry.RecordError(new PipelineError(
            DateTimeOffset.UtcNow, traceId, PipelineName, errorCode, ex.Message, ex.StackTrace ?? string.Empty));

    private async Task EnsureCuratedTopicsAsync()
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

        var specs = CurationCatalog.All
            .Select(s => new TopicSpecification
            {
                Name = s.CuratedTopic,
                NumPartitions = 1,
                ReplicationFactor = options.ReplicationFactor,
            })
            .ToList();

        try
        {
            await admin.CreateTopicsAsync(specs).ConfigureAwait(false);
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            // Some or all curated topics already existed — fine.
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Curation engine started (group={Group}, drain={Drain}).")]
    private static partial void LogStarted(ILogger logger, string group, bool drain);

    [LoggerMessage(Level = LogLevel.Information, Message = "Curation run complete — {Total} curated record(s) produced.")]
    private static partial void LogDrained(ILogger logger, int total);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka consume error: {Reason}")]
    private static partial void LogConsumeError(ILogger logger, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipped an unparseable message on {Topic}: {Error}")]
    private static partial void LogParseError(ILogger logger, string topic, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipped an unprojectable record on {Topic}: {Error}")]
    private static partial void LogProjectError(ILogger logger, string topic, string error);
}
