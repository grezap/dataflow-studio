using Avro.Generic;
using DataFlowStudio.Modules.Warehouse;
using DataFlowStudio.Modules.Warehouse.Sink;
using DataFlowStudio.SharedKernel.Lineage;
using DataFlowStudio.UnitTests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.Kafka;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The DWH sink engine's load orchestration (<c>LoadStarAsync</c>, split from the Kafka consume so it is
/// testable): it loads the Kimball star in dependency order, emits a <c>warehouse-sink</c> stage event
/// per loader, returns the per-entity curated count, and turns a loader failure into a structured error
/// before rethrowing (ADR-0008).
/// </summary>
public sealed class WarehouseSinkEngineTests
{
    private static readonly string[] Topics =
    [
        "dfs.customers.changed.v1", "dfs.product-categories.changed.v1", "dfs.products.changed.v1",
        "dfs.warehouses.changed.v1", "dfs.customer-addresses.changed.v1", "dfs.orders.changed.v1",
        "dfs.order-lines.changed.v1", "dfs.transactions.changed.v1", "dfs.shipments.changed.v1",
        "dfs.product-inventory.changed.v1",
    ];

    private static readonly string[] ExpectedStages =
    [
        "dim_date", "dim_warehouse", "dim_carrier", "dim_customer", "dim_product",
        "fact_order", "fact_order_line", "fact_transaction", "fact_inventory_snap",
    ];

    private static WarehouseSinkEngine Engine(RecordingTelemetrySink telemetry) =>
        new(
            new WarehouseSinkOptions
            {
                Kafka = new KafkaConnectionOptions
                {
                    BootstrapServers = "unused:9092",
                    CaCertPem = "ca",
                    ClientCertPem = "cert",
                    ClientKeyPem = "key",
                },
                SchemaRegistryUrl = "https://unused",
                StarRocksConnection = "Server=unused",
            },
            NullLogger<WarehouseSinkEngine>.Instance,
            telemetry,
            NullLineageEmitter.Instance);

    private static Dictionary<string, Dictionary<string, GenericRecord>> ByTopic(
        params (string Topic, string Key, GenericRecord Record)[] records)
    {
        var byTopic = Topics.ToDictionary(t => t, _ => new Dictionary<string, GenericRecord>(StringComparer.Ordinal), StringComparer.Ordinal);
        foreach (var (topic, key, record) in records)
        {
            byTopic[topic][key] = record;
        }

        return byTopic;
    }

    [Fact]
    public async Task LoadStar_runs_every_stage_in_dependency_order_and_counts_entities()
    {
        var telemetry = new RecordingTelemetrySink();
        var client = new FakeStarRocksClient();

        var customer = AvroRecord.Of(
            "customer",
            ("customerId", "long", 100L), ("customerCode", "string", "C1"), ("displayName", "string", "Ada"),
            ("email", "string", "a@x.io"), ("preferredLocale", "string", "en"), ("status", "int", 1),
            ("lifetimeValueUsd", "string", "10.00"));
        var warehouse = AvroRecord.Of(
            "warehouse",
            ("warehouseId", "int", 7), ("code", "string", "SEA"), ("name", "string", "Seattle"),
            ("region", "string", "W"), ("countryIso2", "string", "US"), ("timezoneIana", "string", "UTC"));
        var order = AvroRecord.Of(
            "order",
            ("orderId", "long", 10L), ("placedAtUtc", "long", new DateTimeOffset(2026, 7, 25, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds()),
            ("customerId", "long", 100L), ("billingAddressId", "long", 1L), ("shippingAddressId", "long", 2L),
            ("status", "int", 3), ("subtotalUsd", "string", "10.00"), ("taxUsd", "string", "1.00"),
            ("shippingUsd", "string", "0.00"), ("totalUsd", "string", "11.00"), ("currency", "string", "USD"));

        var byTopic = ByTopic(
            ("dfs.customers.changed.v1", "100", customer),
            ("dfs.warehouses.changed.v1", "7", warehouse),
            ("dfs.orders.changed.v1", "10", order));

        var counts = await Engine(telemetry).LoadStarAsync(byTopic, client, "trace-abc", new DateTime(2026, 7, 25, 0, 0, 0, DateTimeKind.Utc), CancellationToken.None);

        // One stage event per loader, in dependency order.
        telemetry.Stages.Select(s => s.Stage).ShouldBe(ExpectedStages);
        telemetry.Stages.ShouldAllBe(s => s.Status == "ok" && s.Pipeline == "warehouse-sink");
        telemetry.Errors.ShouldBeEmpty();

        // Per-entity counts (all 10 topics represented; empties are 0).
        counts["customers"].ShouldBe(1);
        counts["warehouses"].ShouldBe(1);
        counts["orders"].ShouldBe(1);
        counts["transactions"].ShouldBe(0);

        // The star was actually written: dims then the truncate-reload fact.
        client.Executed.ShouldContain(s => s.StartsWith("INSERT INTO dwh.dim_customer", StringComparison.Ordinal));
        client.Executed.ShouldContain("TRUNCATE TABLE dwh.fact_order");
    }

    [Fact]
    public async Task LoadStar_turns_a_loader_failure_into_a_structured_error_then_rethrows()
    {
        var telemetry = new RecordingTelemetrySink();
        var client = new FakeStarRocksClient { ThrowOnExecuteContaining = "dwh.dim_warehouse" };

        var warehouse = AvroRecord.Of(
            "warehouse",
            ("warehouseId", "int", 7), ("code", "string", "SEA"), ("name", "string", "Seattle"),
            ("region", "string", "W"), ("countryIso2", "string", "US"), ("timezoneIana", "string", "UTC"));
        var byTopic = ByTopic(("dfs.warehouses.changed.v1", "7", warehouse));

        await Should.ThrowAsync<InvalidOperationException>(() =>
            Engine(telemetry).LoadStarAsync(byTopic, client, "trace-fail", new DateTime(2026, 7, 25, 0, 0, 0, DateTimeKind.Utc), CancellationToken.None));

        var error = telemetry.Errors.ShouldHaveSingleItem();
        error.ErrorCode.ShouldBe("dim_warehouse-load-failed");
        error.Service.ShouldBe("warehouse-sink");
        error.TraceId.ShouldBe("trace-fail");
        telemetry.Flushes.ShouldBeGreaterThan(0);   // errors are flushed before the rethrow
    }
}
