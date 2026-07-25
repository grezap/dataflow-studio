using DataFlowStudio.Modules.Warehouse.Sink;
using DataFlowStudio.UnitTests.Fakes;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The DWH fact loaders (ADR-0006) against a recording <see cref="IStarRocksClient"/>: each fact is a
/// truncate-and-reload of the current snapshot, creating the range partitions the batch needs, with
/// surrogate keys resolved from the dimension lookups and <c>line_total</c> recomputed (the OLTP column
/// is computed-persisted and lands NULL through CDC).
/// </summary>
public sealed class FactLoaderTests
{
    private static readonly DateTime Batch = new(2026, 7, 25, 12, 0, 0, DateTimeKind.Utc);

    private static long Ms(int y, int m, int d) => new DateTimeOffset(y, m, d, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();

    private static SinkLookups Lookups(
        Dictionary<long, long>? customer = null,
        Dictionary<long, long>? product = null,
        Dictionary<long, long>? warehouse = null,
        Dictionary<long, int>? orderDate = null,
        Dictionary<long, long>? orderCustomer = null) =>
        new(customer ?? [], product ?? [], warehouse ?? [], orderDate ?? [], orderCustomer ?? []);

    [Fact]
    public async Task LoadOrders_truncates_creates_partition_and_resolves_customer_sk()
    {
        var client = new FakeStarRocksClient();
        var order = AvroRecord.Of(
            "order",
            ("orderId", "long", 10L),
            ("placedAtUtc", "long", Ms(2026, 7, 25)),
            ("customerId", "long", 100L),
            ("billingAddressId", "long", 1L),
            ("shippingAddressId", "long", 2L),
            ("status", "int", 3),
            ("subtotalUsd", "string", "50.00"),
            ("taxUsd", "string", "5.00"),
            ("shippingUsd", "string", "0.00"),
            ("totalUsd", "string", "55.00"),
            ("currency", "string", "USD"));

        await new FactLoader(client).LoadOrdersAsync([order], Lookups(customer: new() { [100] = 500 }));

        client.Executed[0].ShouldBe("TRUNCATE TABLE dwh.fact_order");
        client.Executed[1].ShouldBe("ALTER TABLE dwh.fact_order ADD PARTITION IF NOT EXISTS p20260725 VALUES [(\"20260725\"), (\"20260726\"))");
        var insert = client.Executed[2];
        insert.ShouldStartWith("INSERT INTO dwh.fact_order");
        insert.ShouldContain("(10, 20260725, 500, 1, 2, 3, 50.00, 5.00, 0.00, 55.00, 'USD', '2026-07-25 00:00:00')");
    }

    [Fact]
    public async Task LoadOrderLines_recomputes_line_total_and_resolves_all_keys()
    {
        var client = new FakeStarRocksClient();
        var line = AvroRecord.Of(
            "orderLine",
            ("orderLineId", "long", 1L),
            ("orderId", "long", 10L),
            ("productId", "long", 200L),
            ("warehouseId", "int", 7),
            ("quantity", "int", 3),
            ("unitPriceUsd", "string", "10.00"),
            ("discountUsd", "string", "5.50"));

        var lookups = Lookups(
            customer: new() { [100] = 500 },
            product: new() { [200] = 600 },
            warehouse: new() { [7] = 700 },
            orderDate: new() { [10] = 20260725 },
            orderCustomer: new() { [10] = 100 });

        await new FactLoader(client).LoadOrderLinesAsync([line], lookups);

        var insert = client.Executed.Last();
        insert.ShouldStartWith("INSERT INTO dwh.fact_order_line");
        // line_total = round(3*10.00 - 5.50, 2) = 24.50; order_date_key + customer_sk via the order.
        insert.ShouldContain("(1, 10, 20260725, 500, 600, 700, 3, 10.00, 5.50, 24.50)");
    }

    [Fact]
    public async Task LoadTransactions_keys_by_its_own_date()
    {
        var client = new FakeStarRocksClient();
        var txn = AvroRecord.Of(
            "transaction",
            ("transactionId", "long", 9L),
            ("orderId", "long", 10L),
            ("occurredAtUtc", "long", Ms(2026, 7, 26)),
            ("provider", "string", "stripe"),
            ("kind", "int", 1),
            ("amountUsd", "string", "55.00"),
            ("status", "int", 2));

        await new FactLoader(client).LoadTransactionsAsync([txn]);

        client.Executed[0].ShouldBe("TRUNCATE TABLE dwh.fact_transaction");
        client.Executed.ShouldContain("ALTER TABLE dwh.fact_transaction ADD PARTITION IF NOT EXISTS p20260726 VALUES [(\"20260726\"), (\"20260727\"))");
        client.Executed.Last().ShouldContain("(9, 10, 20260726, 'stripe', 1, 55.00, 2, '2026-07-26 00:00:00')");
    }

    [Fact]
    public async Task LoadInventory_truncates_and_snapshots_as_of_batch_date()
    {
        var client = new FakeStarRocksClient();
        var inv = AvroRecord.Of(
            "inventory",
            ("productId", "long", 200L),
            ("warehouseId", "int", 7),
            ("onHand", "int", 40),
            ("reserved", "int", 5),
            ("reorderPoint", "int", 10),
            ("safetyStock", "int", 3));

        await new FactLoader(client).LoadInventoryAsync(
            [inv], Lookups(product: new() { [200] = 600 }, warehouse: new() { [7] = 700 }), Batch);

        client.Executed[0].ShouldBe("TRUNCATE TABLE dwh.fact_inventory_snap");
        client.Executed[1].ShouldStartWith("INSERT INTO dwh.fact_inventory_snap");
        client.Executed[1].ShouldContain("(20260725, 600, 700, 40, 5, 10, 3)");
    }

    [Fact]
    public async Task Zero_date_key_is_not_partitioned()
    {
        var client = new FakeStarRocksClient();
        var line = AvroRecord.Of(
            "orderLine",
            ("orderLineId", "long", 1L),
            ("orderId", "long", 10L),
            ("productId", "long", 200L),
            ("warehouseId", "int", 7),
            ("quantity", "int", 1),
            ("unitPriceUsd", "string", "1.00"),
            ("discountUsd", "string", "0.00"));

        // No order-date lookup → dk resolves to 0 → must NOT emit an ADD PARTITION p0.
        await new FactLoader(client).LoadOrderLinesAsync([line], Lookups());

        client.Executed.ShouldNotContain(s => s.Contains("PARTITION", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Empty_inputs_issue_no_statements()
    {
        var client = new FakeStarRocksClient();
        var loader = new FactLoader(client);
        await loader.LoadOrdersAsync([], Lookups());
        await loader.LoadOrderLinesAsync([], Lookups());
        await loader.LoadTransactionsAsync([]);
        await loader.LoadInventoryAsync([], Lookups(), Batch);
        client.Executed.ShouldBeEmpty();
    }
}
