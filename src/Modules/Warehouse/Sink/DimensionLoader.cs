using System.Globalization;
using Avro.Generic;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// Loads the Kimball dimensions into StarRocks from curated records: generated <c>dim_date</c>,
/// SCD1 upserts for <c>dim_warehouse</c>/<c>dim_carrier</c>, and SCD2 (close-current + insert-new-
/// version) for <c>dim_customer</c>/<c>dim_product</c>. All inserts are batched into a single
/// multi-row INSERT per table — StarRocks penalises many single-row inserts (one version each), so
/// batching is required, not just an optimisation. Idempotent: SCD2 inserts a new version only when
/// the tracked attributes actually change, so a re-run with the same data is a no-op.
/// </summary>
public sealed class DimensionLoader(StarRocksClient client)
{
    private const string ValidToMax = "9999-12-31 00:00:00";

    /// <summary>Generates <c>dim_date</c> rows for the given date keys (PK upsert — safe to re-run).</summary>
    public async Task LoadDatesAsync(IReadOnlyCollection<int> dateKeys)
    {
        if (dateKeys.Count == 0)
        {
            return;
        }

        var rows = new List<string>();
        foreach (var dk in dateKeys.Distinct())
        {
            var d = new DateTime(dk / 10000, (dk / 100) % 100, dk % 100, 0, 0, 0, DateTimeKind.Utc);
            int dow = (int)d.DayOfWeek == 0 ? 7 : (int)d.DayOfWeek;   // Mon=1 … Sun=7
            bool weekend = d.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday;
            int isoWeek = System.Globalization.ISOWeek.GetWeekOfYear(d);
            rows.Add($"({dk}, '{d:yyyy-MM-dd}', {d.Year}, {(d.Month - 1) / 3 + 1}, {d.Month}, {d.Day}, {dow}, {(weekend ? 1 : 0)}, {isoWeek})");
        }

        await client.ExecuteAsync(
            "INSERT INTO dwh.dim_date (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend, iso_week) VALUES "
            + string.Join(",", rows)).ConfigureAwait(false);
    }

    /// <summary>SCD1 upsert of <c>dim_warehouse</c> (surrogate key = the natural warehouse id).</summary>
    public async Task LoadWarehousesAsync(IReadOnlyCollection<GenericRecord> warehouses)
    {
        if (warehouses.Count == 0)
        {
            return;
        }

        var rows = warehouses.Select(w =>
            $"({Rec.Int(w, "warehouseId")}, {Rec.Int(w, "warehouseId")}, {Sql.Str(Rec.Str(w, "code"))}, {Sql.Str(Rec.Str(w, "name"))}, "
            + $"{Sql.Str(Rec.Str(w, "region"))}, {Sql.Str(Rec.Str(w, "countryIso2"))}, {Sql.Str(Rec.Str(w, "timezoneIana"))})");

        await client.ExecuteAsync(
            "INSERT INTO dwh.dim_warehouse (warehouse_sk, warehouse_id, code, name, region, country_iso2, timezone_iana) VALUES "
            + string.Join(",", rows)).ConfigureAwait(false);
    }

    /// <summary>SCD1 upsert of <c>dim_carrier</c> from the distinct carriers seen on shipments.</summary>
    public async Task LoadCarriersAsync(IReadOnlyCollection<GenericRecord> shipments)
    {
        var carriers = shipments.Select(s => Rec.Str(s, "carrier")).Distinct(StringComparer.Ordinal).ToList();
        if (carriers.Count == 0)
        {
            return;
        }

        var existing = await client.StringLongMapAsync("SELECT carrier, carrier_sk FROM dwh.dim_carrier").ConfigureAwait(false);
        long nextSk = await client.ScalarLongAsync("SELECT COALESCE(MAX(carrier_sk),0) FROM dwh.dim_carrier").ConfigureAwait(false) + 1;

        var rows = new List<string>();
        foreach (var carrier in carriers.Where(c => !existing.ContainsKey(c)))
        {
            rows.Add($"({nextSk++}, {Sql.Str(carrier)}, {Sql.Str("standard")})");
        }

        if (rows.Count > 0)
        {
            await client.ExecuteAsync("INSERT INTO dwh.dim_carrier (carrier_sk, carrier, service_level) VALUES " + string.Join(",", rows))
                .ConfigureAwait(false);
        }
    }

    /// <summary>SCD2 load of <c>dim_customer</c>.</summary>
    public Task LoadCustomersAsync(IReadOnlyCollection<GenericRecord> customers, DateTime batchUtc) =>
        LoadScd2Async(
            "dim_customer", "customer_sk", "customer_id", "customerId",
            customers, batchUtc,
            columns: "customer_id, customer_code, display_name, email, preferred_locale, status, lifetime_value_usd",
            businessValues: c =>
            [
                Sql.Num(Rec.Long(c, "customerId")), Sql.Str(Rec.Str(c, "customerCode")), Sql.Str(Rec.Str(c, "displayName")),
                Sql.Str(Rec.Str(c, "email")), Sql.Str(Rec.Str(c, "preferredLocale")), Sql.Num(Rec.Int(c, "status")),
                Sql.Decimal(Rec.Str(c, "lifetimeValueUsd")),
            ],
            attributeSelect: "customer_code, display_name, email, preferred_locale, status, CAST(lifetime_value_usd AS CHAR)",
            signature: c => string.Join('|',
                Rec.Str(c, "customerCode"), Rec.Str(c, "displayName"), Rec.Str(c, "email"),
                Rec.Str(c, "preferredLocale"), Rec.Int(c, "status"), Decimal(Rec.Str(c, "lifetimeValueUsd"))));

    /// <summary>SCD2 load of <c>dim_product</c> (category name resolved from the category map).</summary>
    public Task LoadProductsAsync(
        IReadOnlyCollection<GenericRecord> products,
        IReadOnlyDictionary<int, string> categoryNames,
        DateTime batchUtc) =>
        LoadScd2Async(
            "dim_product", "product_sk", "product_id", "productId",
            products, batchUtc,
            columns: "product_id, sku, category_id, category_name, display_name, list_price_usd",
            businessValues: p =>
            [
                Sql.Num(Rec.Long(p, "productId")), Sql.Str(Rec.Str(p, "sku")), Sql.Num(Rec.Int(p, "categoryId")),
                Sql.Str(categoryNames.GetValueOrDefault(Rec.Int(p, "categoryId"), string.Empty)),
                Sql.Str(Rec.Str(p, "displayName")), Sql.Decimal(Rec.Str(p, "listPriceUsd")),
            ],
            attributeSelect: "sku, category_id, category_name, display_name, CAST(list_price_usd AS CHAR)",
            signature: p => string.Join('|',
                Rec.Str(p, "sku"), Rec.Int(p, "categoryId"), categoryNames.GetValueOrDefault(Rec.Int(p, "categoryId"), string.Empty),
                Rec.Str(p, "displayName"), Decimal(Rec.Str(p, "listPriceUsd"))));

    // The shared SCD2 mechanics: for each record, compare its attribute signature to the current
    // version's; unchanged → skip; changed/absent → close the old (if any) and insert a new version.
    private async Task LoadScd2Async(
        string table, string skColumn, string bkColumn, string bkField,
        IReadOnlyCollection<GenericRecord> records, DateTime batchUtc,
        string columns, Func<GenericRecord, string[]> businessValues,
        string attributeSelect, Func<GenericRecord, string> signature)
    {
        if (records.Count == 0)
        {
            return;
        }

        long nextSk = await client.ScalarLongAsync($"SELECT COALESCE(MAX({skColumn}),0) FROM dwh.{table}").ConfigureAwait(false) + 1;
        string batch = batchUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        var newRows = new List<string>();
        var toClose = new List<long>();

        foreach (var record in records)
        {
            long bk = Rec.Long(record, bkField);
            var current = await client.RowAsync(
                $"SELECT {skColumn}, {attributeSelect} FROM dwh.{table} WHERE {bkColumn} = {bk} AND is_current = 1").ConfigureAwait(false);

            var newSig = signature(record);
            if (current is not null)
            {
                var oldSig = string.Join('|', current.Skip(1).Select(NormalizeSignaturePart));
                if (string.Equals(oldSig, newSig, StringComparison.Ordinal))
                {
                    continue;   // unchanged — idempotent no-op
                }

                toClose.Add(Convert.ToInt64(current[0], CultureInfo.InvariantCulture));
            }

            var values = businessValues(record);
            newRows.Add($"({nextSk++}, {string.Join(", ", values)}, '{batch}', '{ValidToMax}', 1)");
        }

        if (toClose.Count > 0)
        {
            await client.ExecuteAsync(
                $"UPDATE dwh.{table} SET is_current = 0, valid_to = '{batch}' WHERE {skColumn} IN ({string.Join(",", toClose)})")
                .ConfigureAwait(false);
        }

        if (newRows.Count > 0)
        {
            await client.ExecuteAsync(
                $"INSERT INTO dwh.{table} ({skColumn}, {columns}, valid_from, valid_to, is_current) VALUES " + string.Join(",", newRows))
                .ConfigureAwait(false);
        }
    }

    // The attribute-select mixes strings/ints/decimals; normalize each part so the old signature
    // matches the new one built from the curated record (decimals compared by value, not formatting).
    private static string NormalizeSignaturePart(object? part)
    {
        var s = part?.ToString() ?? string.Empty;
        return decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var dec)
            ? dec.ToString(CultureInfo.InvariantCulture)
            : s;
    }

    private static string Decimal(string s) =>
        decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d.ToString(CultureInfo.InvariantCulture) : s;
}
