using System.Text.Json;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
/// </summary>
public sealed partial class CurationEngine(CurationOptions options, ILogger<CurationEngine> logger)
{
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
                if (await TryCurateAsync(producer, result, counts, cancellationToken).ConfigureAwait(false))
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
        LogDrained(logger, total);
        return counts;
    }

    private async Task<bool> TryCurateAsync(
        IProducer<string, GenericRecord> producer,
        ConsumeResult<string, string> result,
        Dictionary<string, int> counts,
        CancellationToken cancellationToken)
    {
        var spec = CurationCatalog.ForRawTopic(result.Topic);
        if (spec is null || result.Message.Value is null)
        {
            return false;   // unknown topic or a tombstone
        }

        DebeziumChange change;
        try
        {
            change = DebeziumChange.Parse(result.Message.Value);
        }
        catch (JsonException ex)
        {
            LogParseError(logger, result.Topic, ex.Message);
            return false;
        }

        if (!change.HasAfter)
        {
            return false;   // hard delete — OltpDb soft-deletes, so this does not occur; skip defensively
        }

        Avro.Generic.GenericRecord record;
        string key;
        try
        {
            (record, key) = CuratedRecordProjector.Project(spec, change);
        }
        catch (InvalidOperationException ex)
        {
            LogProjectError(logger, result.Topic, ex.Message);   // malformed record — skip, don't crash the run
            return false;
        }

        await producer.ProduceAsync(
            spec.CuratedTopic,
            new Message<string, GenericRecord> { Key = key, Value = record },
            cancellationToken).ConfigureAwait(false);

        counts[spec.Entity]++;
        return true;
    }

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
