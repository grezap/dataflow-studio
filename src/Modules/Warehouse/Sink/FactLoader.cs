using System.Globalization;
using Avro.Generic;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>Dimension surrogate-key lookups + order context the fact loaders resolve against.</summary>
/// <param name="CustomerSkByCustomerId">Current customer surrogate key by business id.</param>
/// <param name="ProductSkByProductId">Current product surrogate key by business id.</param>
/// <param name="WarehouseSkByWarehouseId">Warehouse surrogate key by business id.</param>
/// <param name="OrderDateKeyByOrderId">Order date key by order id (for order-line facts).</param>
/// <param name="OrderCustomerIdByOrderId">Order's customer id by order id (for order-line customer_sk).</param>
public sealed record SinkLookups(
    IReadOnlyDictionary<long, long> CustomerSkByCustomerId,
    IReadOnlyDictionary<long, long> ProductSkByProductId,
    IReadOnlyDictionary<long, long> WarehouseSkByWarehouseId,
    IReadOnlyDictionary<long, int> OrderDateKeyByOrderId,
    IReadOnlyDictionary<long, long> OrderCustomerIdByOrderId);

/// <summary>
/// Loads the DWH facts from curated records. Facts are DUPLICATE KEY tables range-partitioned by
/// their date key (except the inventory snapshot), so each loader truncates for an idempotent full
/// reload of the current snapshot, creates the partitions the batch needs, then batch-inserts.
/// Incremental (per-change) fact loading is a later enhancement; the drain reloads the snapshot.
/// </summary>
public sealed class FactLoader(StarRocksClient client)
{
    /// <summary>fact_order — one row per order, with the customer surrogate key + order date key.</summary>
    public async Task LoadOrdersAsync(IReadOnlyCollection<GenericRecord> orders, SinkLookups lookups)
    {
        if (orders.Count == 0)
        {
            return;
        }

        var rows = new List<string>();
        var dateKeys = new HashSet<int>();
        foreach (var o in orders)
        {
            int dk = Sql.DateKey(Rec.Long(o, "placedAtUtc"));
            dateKeys.Add(dk);
            long customerSk = lookups.CustomerSkByCustomerId.GetValueOrDefault(Rec.Long(o, "customerId"), 0);
            rows.Add(
                $"({Rec.Long(o, "orderId")}, {dk}, {customerSk}, {Rec.Long(o, "billingAddressId")}, {Rec.Long(o, "shippingAddressId")}, "
                + $"{Rec.Int(o, "status")}, {Sql.Decimal(Rec.Str(o, "subtotalUsd"))}, {Sql.Decimal(Rec.Str(o, "taxUsd"))}, "
                + $"{Sql.Decimal(Rec.Str(o, "shippingUsd"))}, {Sql.Decimal(Rec.Str(o, "totalUsd"))}, {Sql.Str(Rec.Str(o, "currency"))}, "
                + $"{Sql.DateTimeUtc(Rec.Long(o, "placedAtUtc"))})");
        }

        await ReloadAsync("fact_order", "order_date_key", dateKeys,
            "order_id, order_date_key, customer_sk, billing_address_id, shipping_address_id, status, subtotal_usd, tax_usd, shipping_usd, total_usd, currency, placed_at_utc",
            rows).ConfigureAwait(false);
    }

    /// <summary>fact_order_line — resolves order date + customer via the order, product/warehouse via dims.</summary>
    public async Task LoadOrderLinesAsync(IReadOnlyCollection<GenericRecord> lines, SinkLookups lookups)
    {
        if (lines.Count == 0)
        {
            return;
        }

        var rows = new List<string>();
        var dateKeys = new HashSet<int>();
        foreach (var l in lines)
        {
            long orderId = Rec.Long(l, "orderId");
            int dk = lookups.OrderDateKeyByOrderId.GetValueOrDefault(orderId, 0);
            dateKeys.Add(dk);
            long customerSk = lookups.CustomerSkByCustomerId.GetValueOrDefault(
                lookups.OrderCustomerIdByOrderId.GetValueOrDefault(orderId, 0), 0);

            decimal unit = ParseDecimal(Rec.Str(l, "unitPriceUsd"));
            decimal disc = ParseDecimal(Rec.Str(l, "discountUsd"));
            int qty = Rec.Int(l, "quantity");
            decimal lineTotal = Math.Round((qty * unit) - disc, 2);

            rows.Add(
                $"({Rec.Long(l, "orderLineId")}, {orderId}, {dk}, {customerSk}, "
                + $"{lookups.ProductSkByProductId.GetValueOrDefault(Rec.Long(l, "productId"), 0)}, "
                + $"{lookups.WarehouseSkByWarehouseId.GetValueOrDefault(Rec.Int(l, "warehouseId"), 0)}, "
                + $"{qty}, {unit.ToString(CultureInfo.InvariantCulture)}, {disc.ToString(CultureInfo.InvariantCulture)}, "
                + $"{lineTotal.ToString(CultureInfo.InvariantCulture)})");
        }

        await ReloadAsync("fact_order_line", "order_date_key", dateKeys,
            "order_line_id, order_id, order_date_key, customer_sk, product_sk, warehouse_sk, quantity, unit_price_usd, discount_usd, line_total_usd",
            rows).ConfigureAwait(false);
    }

    /// <summary>fact_transaction — one row per payment transaction, keyed by its own date.</summary>
    public async Task LoadTransactionsAsync(IReadOnlyCollection<GenericRecord> transactions)
    {
        if (transactions.Count == 0)
        {
            return;
        }

        var rows = new List<string>();
        var dateKeys = new HashSet<int>();
        foreach (var t in transactions)
        {
            int dk = Sql.DateKey(Rec.Long(t, "occurredAtUtc"));
            dateKeys.Add(dk);
            rows.Add(
                $"({Rec.Long(t, "transactionId")}, {Rec.Long(t, "orderId")}, {dk}, {Sql.Str(Rec.Str(t, "provider"))}, "
                + $"{Rec.Int(t, "kind")}, {Sql.Decimal(Rec.Str(t, "amountUsd"))}, {Rec.Int(t, "status")}, "
                + $"{Sql.DateTimeUtc(Rec.Long(t, "occurredAtUtc"))})");
        }

        await ReloadAsync("fact_transaction", "txn_date_key", dateKeys,
            "transaction_id, order_id, txn_date_key, provider, kind, amount_usd, status, occurred_at_utc",
            rows).ConfigureAwait(false);
    }

    /// <summary>fact_inventory_snap — a snapshot as of today (unpartitioned); truncate + reload.</summary>
    public async Task LoadInventoryAsync(IReadOnlyCollection<GenericRecord> inventory, SinkLookups lookups, DateTime batchUtc)
    {
        if (inventory.Count == 0)
        {
            return;
        }

        int snapKey = (batchUtc.Year * 10000) + (batchUtc.Month * 100) + batchUtc.Day;
        var rows = inventory.Select(i =>
            $"({snapKey}, {lookups.ProductSkByProductId.GetValueOrDefault(Rec.Long(i, "productId"), 0)}, "
            + $"{lookups.WarehouseSkByWarehouseId.GetValueOrDefault(Rec.Int(i, "warehouseId"), 0)}, "
            + $"{Rec.Int(i, "onHand")}, {Rec.Int(i, "reserved")}, {Rec.Int(i, "reorderPoint")}, {Rec.Int(i, "safetyStock")})");

        await client.ExecuteAsync("TRUNCATE TABLE dwh.fact_inventory_snap").ConfigureAwait(false);
        await client.ExecuteAsync(
            "INSERT INTO dwh.fact_inventory_snap (snap_date_key, product_sk, warehouse_sk, on_hand, reserved, reorder_point, safety_stock) VALUES "
            + string.Join(",", rows)).ConfigureAwait(false);
    }

    // Truncate (idempotent full reload), ensure the range partitions the batch needs, then insert.
    private async Task ReloadAsync(string table, string partitionColumn, IReadOnlyCollection<int> dateKeys, string columns, IReadOnlyCollection<string> rows)
    {
        await client.ExecuteAsync($"TRUNCATE TABLE dwh.{table}").ConfigureAwait(false);
        foreach (var dk in dateKeys.Where(k => k > 0).Distinct())
        {
            await client.ExecuteAsync(
                $"ALTER TABLE dwh.{table} ADD PARTITION IF NOT EXISTS p{dk} VALUES [(\"{dk}\"), (\"{dk + 1}\"))").ConfigureAwait(false);
        }

        await client.ExecuteAsync($"INSERT INTO dwh.{table} ({columns}) VALUES " + string.Join(",", rows)).ConfigureAwait(false);
    }

    private static decimal ParseDecimal(string s) =>
        decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : 0m;
}
