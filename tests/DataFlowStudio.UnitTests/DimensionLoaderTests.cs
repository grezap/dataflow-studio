using DataFlowStudio.Modules.Warehouse.Sink;
using DataFlowStudio.UnitTests.Fakes;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The Kimball dimension loaders (ADR-0006) against a recording <see cref="IStarRocksClient"/>:
/// generated <c>dim_date</c>, SCD1 <c>dim_warehouse</c>/<c>dim_carrier</c>, and the SCD2 mechanics for
/// <c>dim_customer</c>/<c>dim_product</c> — unchanged is a no-op, a changed attribute closes the current
/// version and inserts a new one. All inserts must be a single batched multi-row statement.
/// </summary>
public sealed class DimensionLoaderTests
{
    private static readonly DateTime Batch = new(2026, 7, 25, 12, 0, 0, DateTimeKind.Utc);

    private static long EpochMillis(int year, int month, int day) =>
        new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();

    [Fact]
    public async Task LoadDates_empty_issues_no_statement()
    {
        var client = new FakeStarRocksClient();
        await new DimensionLoader(client).LoadDatesAsync([]);
        client.Executed.ShouldBeEmpty();
    }

    [Fact]
    public async Task LoadDates_derives_quarter_weekday_and_weekend()
    {
        var client = new FakeStarRocksClient();
        // 2026-07-25 is a Saturday in Q3, ISO week 30.
        await new DimensionLoader(client).LoadDatesAsync([20260725]);

        var sql = client.Executed.ShouldHaveSingleItem();
        sql.ShouldStartWith("INSERT INTO dwh.dim_date");
        // (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend, iso_week)
        sql.ShouldContain("(20260725, '2026-07-25', 2026, 3, 7, 25, 6, 1, 30)");
    }

    [Fact]
    public async Task LoadDates_deduplicates_repeated_keys()
    {
        var client = new FakeStarRocksClient();
        await new DimensionLoader(client).LoadDatesAsync([20260101, 20260101]);

        var sql = client.Executed.ShouldHaveSingleItem();
        // Monday 2026-01-01? No — 2026-01-01 is a Thursday (dow 4), not weekend, Q1, iso week 1.
        sql.ShouldContain("(20260101, '2026-01-01', 2026, 1, 1, 1, 4, 0, 1)");
        // Only one tuple → no comma-joined duplicate.
        sql.Split("),(").Length.ShouldBe(1);
    }

    [Fact]
    public async Task LoadWarehouses_uses_the_natural_id_as_surrogate_key()
    {
        var client = new FakeStarRocksClient();
        var wh = AvroRecord.Of(
            "warehouse",
            ("warehouseId", "int", 7),
            ("code", "string", "SEA"),
            ("name", "string", "Seattle"),
            ("region", "string", "West"),
            ("countryIso2", "string", "US"),
            ("timezoneIana", "string", "America/Los_Angeles"));

        await new DimensionLoader(client).LoadWarehousesAsync([wh]);

        var sql = client.Executed.ShouldHaveSingleItem();
        sql.ShouldStartWith("INSERT INTO dwh.dim_warehouse");
        sql.ShouldContain("(7, 7, 'SEA', 'Seattle', 'West', 'US', 'America/Los_Angeles')");
    }

    [Fact]
    public async Task LoadCarriers_appends_only_new_carriers_from_max_sk()
    {
        var client = new FakeStarRocksClient
        {
            OnStringLongMap = _ => new Dictionary<string, long>(StringComparer.Ordinal) { ["UPS"] = 1 },
            OnScalarLong = _ => 1,   // MAX(carrier_sk) = 1 → next = 2
        };
        var ups = AvroRecord.Of("shipment", ("carrier", "string", "UPS"));
        var fedex = AvroRecord.Of("shipment", ("carrier", "string", "FedEx"));

        await new DimensionLoader(client).LoadCarriersAsync([ups, fedex]);

        var sql = client.Executed.ShouldHaveSingleItem();
        sql.ShouldStartWith("INSERT INTO dwh.dim_carrier");
        sql.ShouldContain("(2, 'FedEx', 'standard')");
        sql.ShouldNotContain("UPS");   // already existed → skipped
    }

    [Fact]
    public async Task LoadCarriers_no_new_carriers_issues_no_insert()
    {
        var client = new FakeStarRocksClient
        {
            OnStringLongMap = _ => new Dictionary<string, long>(StringComparer.Ordinal) { ["UPS"] = 1 },
        };
        var ups = AvroRecord.Of("shipment", ("carrier", "string", "UPS"));

        await new DimensionLoader(client).LoadCarriersAsync([ups]);

        client.Executed.ShouldBeEmpty();
    }

    private static Avro.Generic.GenericRecord Customer(string code, string name, string email, string locale, int status, string ltv) =>
        AvroRecord.Of(
            "customer",
            ("customerId", "long", 100L),
            ("customerCode", "string", code),
            ("displayName", "string", name),
            ("email", "string", email),
            ("preferredLocale", "string", locale),
            ("status", "int", status),
            ("lifetimeValueUsd", "string", ltv));

    [Fact]
    public async Task Scd2_first_version_inserts_current_row_from_max_sk()
    {
        var client = new FakeStarRocksClient { OnScalarLong = _ => 4 };   // next sk = 5

        await new DimensionLoader(client).LoadCustomersAsync(
            [Customer("C100", "Ada", "ada@x.io", "en-US", 1, "100.00")], Batch);

        var insert = client.Executed.ShouldHaveSingleItem();
        insert.ShouldStartWith("INSERT INTO dwh.dim_customer");
        insert.ShouldContain("(5, 100, 'C100', 'Ada', 'ada@x.io', 'en-US', 1, 100.00, '2026-07-25 12:00:00', '9999-12-31 00:00:00', 1)");
    }

    [Fact]
    public async Task Scd2_unchanged_attributes_are_a_no_op()
    {
        var client = new FakeStarRocksClient
        {
            OnScalarLong = _ => 5,
            // current version: [sk, code, name, email, locale, status, ltv-as-char]. StarRocks CAST of a
            // DECIMAL(_,2) yields the same 2dp scale the curated value carries, so signatures compare equal.
            OnRow = _ => [5L, "C100", "Ada", "ada@x.io", "en-US", 1, "100.00"],
        };

        await new DimensionLoader(client).LoadCustomersAsync(
            [Customer("C100", "Ada", "ada@x.io", "en-US", 1, "100.00")], Batch);

        client.Executed.ShouldBeEmpty();   // every attribute matched → nothing written
    }

    [Fact]
    public async Task Scd2_changed_attribute_closes_current_and_inserts_new_version()
    {
        var client = new FakeStarRocksClient
        {
            OnScalarLong = _ => 5,   // next sk = 6
            OnRow = _ => [5L, "C100", "Ada", "ada@x.io", "en-US", 1, "100.00"],
        };

        // email changed → new version.
        await new DimensionLoader(client).LoadCustomersAsync(
            [Customer("C100", "Ada", "ada.new@x.io", "en-US", 1, "100.00")], Batch);

        client.Executed.Count.ShouldBe(2);
        client.Executed[0].ShouldBe(
            "UPDATE dwh.dim_customer SET is_current = 0, valid_to = '2026-07-25 12:00:00' WHERE customer_sk IN (5)");
        client.Executed[1].ShouldStartWith("INSERT INTO dwh.dim_customer");
        client.Executed[1].ShouldContain("(6, 100, 'C100', 'Ada', 'ada.new@x.io', 'en-US', 1, 100.00,");
    }

    [Fact]
    public async Task Scd2_products_resolve_category_name_from_the_map()
    {
        var client = new FakeStarRocksClient { OnScalarLong = _ => 0 };   // next sk = 1
        var product = AvroRecord.Of(
            "product",
            ("productId", "long", 200L),
            ("sku", "string", "SKU-1"),
            ("categoryId", "int", 3),
            ("displayName", "string", "Widget"),
            ("listPriceUsd", "string", "9.99"));
        var categories = new Dictionary<int, string> { [3] = "Gadgets" };

        await new DimensionLoader(client).LoadProductsAsync([product], categories, Batch);

        var insert = client.Executed.ShouldHaveSingleItem();
        insert.ShouldStartWith("INSERT INTO dwh.dim_product");
        insert.ShouldContain("(1, 200, 'SKU-1', 3, 'Gadgets', 'Widget', 9.99,");
    }

    [Fact]
    public async Task Scd2_empty_input_is_a_no_op()
    {
        var client = new FakeStarRocksClient();
        await new DimensionLoader(client).LoadCustomersAsync([], Batch);
        client.Executed.ShouldBeEmpty();
    }
}
