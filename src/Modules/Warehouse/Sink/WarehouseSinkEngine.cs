using System.Diagnostics;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Logging;
using Nexus.Avro;
using Nexus.Kafka;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// The StarRocks DWH sink engine: consumes the curated Avro topics into memory (deduplicated by
/// message key — at-least-once delivery means duplicates), then loads the Kimball star in dependency
/// order — dimensions before facts, categories before products, orders before order lines — so every
/// surrogate-key lookup is available when a fact needs it. Runs in drain mode (load the current
/// snapshot, then stop) for the runnable sink + the live load; the hosted worker calls it on a timer.
/// <para>
/// Each loader stage is timed and emitted as a <c>warehouse-sink</c> pipeline event through the
/// injected <see cref="IPipelineTelemetrySink"/> (a no-op when unwired); a failing stage emits a
/// structured error before rethrowing (ADR-0008).
/// </para>
/// </summary>
public sealed partial class WarehouseSinkEngine(
    WarehouseSinkOptions options,
    ILogger<WarehouseSinkEngine> logger,
    IPipelineTelemetrySink telemetry)
{
    private const string Prefix = "dfs.";
    private const string PipelineName = "warehouse-sink";

    private static readonly string[] Topics =
    [
        "dfs.customers.changed.v1", "dfs.product-categories.changed.v1", "dfs.products.changed.v1",
        "dfs.warehouses.changed.v1", "dfs.customer-addresses.changed.v1", "dfs.orders.changed.v1",
        "dfs.order-lines.changed.v1", "dfs.transactions.changed.v1", "dfs.shipments.changed.v1",
        "dfs.product-inventory.changed.v1",
    ];

    /// <summary>
    /// Consumes the curated snapshot and loads the DWH. Returns the per-topic count of curated records
    /// consumed. Idempotent: dimensions upsert / SCD2, facts truncate-and-reload the current snapshot.
    /// </summary>
    /// <param name="cancellationToken">Stops the consume loop.</param>
    public async Task<IReadOnlyDictionary<string, int>> RunAsync(CancellationToken cancellationToken)
    {
        var byTopic = ConsumeSnapshot(cancellationToken);

        await using var client = new StarRocksClient(options.StarRocksConnection);
        await client.OpenAsync().ConfigureAwait(false);

        var dim = new DimensionLoader(client);
        var fact = new FactLoader(client);
        var batchUtc = DateTime.UtcNow;
        var traceId = Guid.NewGuid().ToString("N");
        var runStopwatch = Stopwatch.StartNew();

        // Times one loader stage, emits a warehouse-sink pipeline event, and re-raises (as an error
        // event) any failure so a bad load is both observable and still fatal to the run.
        async Task Stage(string stage, Func<Task> load)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                await load().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                telemetry.RecordError(new PipelineError(
                    DateTimeOffset.UtcNow, traceId, PipelineName, $"{stage}-load-failed", ex.Message, ex.StackTrace ?? string.Empty));
                await telemetry.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                throw;
            }

            stopwatch.Stop();
            telemetry.RecordStage(new PipelineStageEvent(
                DateTimeOffset.UtcNow, traceId, PipelineName, stage, "ok", (uint)stopwatch.ElapsedMilliseconds, "{}"));
        }

        IReadOnlyCollection<GenericRecord> Records(string topic) => byTopic[topic].Values;

        var orders = Records("dfs.orders.changed.v1");
        var transactions = Records("dfs.transactions.changed.v1");

        // dim_date — every date any fact references, plus today's inventory-snapshot date.
        var dateKeys = new HashSet<int>();
        foreach (var o in orders)
        {
            dateKeys.Add(Sql.DateKey(Rec.Long(o, "placedAtUtc")));
        }

        foreach (var t in transactions)
        {
            dateKeys.Add(Sql.DateKey(Rec.Long(t, "occurredAtUtc")));
        }

        dateKeys.Add((batchUtc.Year * 10000) + (batchUtc.Month * 100) + batchUtc.Day);

        await Stage("dim_date", () => dim.LoadDatesAsync(dateKeys)).ConfigureAwait(false);
        await Stage("dim_warehouse", () => dim.LoadWarehousesAsync(Records("dfs.warehouses.changed.v1"))).ConfigureAwait(false);
        await Stage("dim_carrier", () => dim.LoadCarriersAsync(Records("dfs.shipments.changed.v1"))).ConfigureAwait(false);

        var categoryNames = new Dictionary<int, string>();
        foreach (var c in Records("dfs.product-categories.changed.v1"))
        {
            categoryNames[Rec.Int(c, "categoryId")] = Rec.Str(c, "name");
        }

        await Stage("dim_customer", () => dim.LoadCustomersAsync(Records("dfs.customers.changed.v1"), batchUtc)).ConfigureAwait(false);
        await Stage("dim_product", () => dim.LoadProductsAsync(Records("dfs.products.changed.v1"), categoryNames, batchUtc)).ConfigureAwait(false);

        // Surrogate-key lookups now that the dimensions are loaded.
        var lookups = new SinkLookups(
            await client.LongMapAsync("SELECT customer_id, customer_sk FROM dwh.dim_customer WHERE is_current = 1").ConfigureAwait(false),
            await client.LongMapAsync("SELECT product_id, product_sk FROM dwh.dim_product WHERE is_current = 1").ConfigureAwait(false),
            await client.LongMapAsync("SELECT warehouse_id, warehouse_sk FROM dwh.dim_warehouse").ConfigureAwait(false),
            orders.ToDictionary(o => Rec.Long(o, "orderId"), o => Sql.DateKey(Rec.Long(o, "placedAtUtc"))),
            orders.ToDictionary(o => Rec.Long(o, "orderId"), o => Rec.Long(o, "customerId")));

        await Stage("fact_order", () => fact.LoadOrdersAsync(orders, lookups)).ConfigureAwait(false);
        await Stage("fact_order_line", () => fact.LoadOrderLinesAsync(Records("dfs.order-lines.changed.v1"), lookups)).ConfigureAwait(false);
        await Stage("fact_transaction", () => fact.LoadTransactionsAsync(transactions)).ConfigureAwait(false);
        await Stage("fact_inventory_snap", () => fact.LoadInventoryAsync(Records("dfs.product-inventory.changed.v1"), lookups, batchUtc)).ConfigureAwait(false);

        var counts = byTopic.ToDictionary(kv => Entity(kv.Key), kv => kv.Value.Count, StringComparer.Ordinal);
        int total = counts.Values.Sum();
        runStopwatch.Stop();

        // A run-summary stage event (the whole load's latency + total records), then flush telemetry.
        telemetry.RecordStage(new PipelineStageEvent(
            DateTimeOffset.UtcNow, traceId, PipelineName, "load", "ok",
            (uint)runStopwatch.ElapsedMilliseconds, $"{{\"records\":{total}}}"));
        await telemetry.FlushAsync(CancellationToken.None).ConfigureAwait(false);

        LogLoaded(logger, total);
        return counts;
    }

    // Consume every curated topic to idle, keeping the latest record per message key (natural key).
    private Dictionary<string, Dictionary<string, GenericRecord>> ConsumeSnapshot(CancellationToken cancellationToken)
    {
        var byTopic = Topics.ToDictionary(t => t, _ => new Dictionary<string, GenericRecord>(StringComparer.Ordinal), StringComparer.Ordinal);

        var srOptions = new SchemaRegistryOptions
        {
            Url = options.SchemaRegistryUrl,
            EnableCertificateVerification = options.VerifySchemaRegistryCertificate,
        };
        using var registry = AvroSerdes.CreateRegistryClient(srOptions);

        var consumerConfig = KafkaClientFactory.CreateConsumerConfig(options.Kafka, options.ConsumerGroup);
        consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
        using var consumer = new ConsumerBuilder<string, GenericRecord>(consumerConfig)
            .SetValueDeserializer(AvroSerdes.CreateDeserializer<GenericRecord>(registry).AsSyncOverAsync())
            .Build();
        consumer.Subscribe(Topics);

        LogConsuming(logger, options.ConsumerGroup);
        var lastMessageUtc = DateTime.UtcNow;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(2));
                if (result is null)
                {
                    if (DateTime.UtcNow - lastMessageUtc > options.DrainIdleTimeout)
                    {
                        break;
                    }

                    continue;
                }

                lastMessageUtc = DateTime.UtcNow;
                if (result.Message.Value is not null && byTopic.TryGetValue(result.Topic, out var byKey))
                {
                    byKey[DedupKey(result.Topic, result.Message.Key, result.Message.Value)] = result.Message.Value;
                }
            }
        }
        finally
        {
            consumer.Close();
        }

        return byTopic;
    }

    // Dedup key = the row's unique natural key. For most topics the Kafka message key already is that
    // key; product-inventory is keyed only by product, so its full grain (product + warehouse) is used.
    private static string DedupKey(string topic, string? messageKey, GenericRecord record) =>
        topic == "dfs.product-inventory.changed.v1"
            ? $"{Rec.Long(record, "productId")}:{Rec.Int(record, "warehouseId")}"
            : messageKey ?? Guid.NewGuid().ToString("N");

    private static string Entity(string topic) =>
        topic.StartsWith(Prefix, StringComparison.Ordinal) ? topic[Prefix.Length..].Replace(".changed.v1", string.Empty, StringComparison.Ordinal) : topic;

    [LoggerMessage(Level = LogLevel.Information, Message = "DWH sink consuming curated topics (group={Group})…")]
    private static partial void LogConsuming(ILogger logger, string group);

    [LoggerMessage(Level = LogLevel.Information, Message = "DWH sink load complete — {Total} curated record(s) consumed and loaded.")]
    private static partial void LogLoaded(ILogger logger, int total);
}
