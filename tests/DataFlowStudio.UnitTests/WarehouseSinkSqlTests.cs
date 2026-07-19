using DataFlowStudio.Modules.Warehouse.Sink;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// Unit tests for the StarRocks SQL value formatting the DWH loaders rely on: date-key derivation,
/// DATETIME literals from epoch millis, decimal literals, and string escaping.
/// </summary>
public sealed class WarehouseSinkSqlTests
{
    private static readonly long Jul1_2026_10h =
        new DateTimeOffset(2026, 7, 1, 10, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();

    [Fact]
    public void DateKey_is_yyyymmdd_in_utc()
    {
        Sql.DateKey(Jul1_2026_10h).ShouldBe(20260701);
    }

    [Fact]
    public void DateTimeUtc_formats_epoch_millis_as_a_quoted_literal()
    {
        Sql.DateTimeUtc(Jul1_2026_10h).ShouldBe("'2026-07-01 10:00:00'");
        Sql.DateTimeUtc(null).ShouldBe("NULL");
    }

    [Fact]
    public void Decimal_passes_through_a_curated_decimal_string_and_defaults_empty_to_zero()
    {
        Sql.Decimal("318.18").ShouldBe("318.18");
        Sql.Decimal("").ShouldBe("0");
        Sql.Decimal(null).ShouldBe("0");
    }

    [Fact]
    public void Str_quotes_and_escapes()
    {
        Sql.Str("Ada").ShouldBe("'Ada'");
        Sql.Str("O'Brien").ShouldBe("'O''Brien'");
        Sql.Str(null).ShouldBe("NULL");
    }

    [Fact]
    public void Num_and_bool_format_as_literals()
    {
        Sql.Num(42L).ShouldBe("42");
        Sql.Num(null).ShouldBe("NULL");
        Sql.Bool(true).ShouldBe("1");
        Sql.Bool(false).ShouldBe("0");
    }
}
