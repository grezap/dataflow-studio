using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// Formats curated values as StarRocks SQL literals. StarRocks' MySQL-protocol server has limited
/// prepared-statement support, so the loaders build INSERT statements with inline literals; every
/// value flows through here (strings escaped) — the inputs are our own curated events, not user text.
/// Curated decimals arrive as strings and timestamps as epoch-millisecond longs (see the curation
/// catalog), which map to StarRocks DECIMAL literals and <c>DATETIME</c> / <c>date_key</c> here.
/// </summary>
public static class Sql
{
    /// <summary>A quoted, escaped string literal (or <c>NULL</c>).</summary>
    public static string Str(object? value)
    {
        if (value is null)
        {
            return "NULL";
        }

        var s = value.ToString() ?? string.Empty;
        return "'" + s.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal) + "'";
    }

    /// <summary>An integer literal (long/int), or <c>NULL</c>.</summary>
    public static string Num(object? value) =>
        value is null ? "NULL" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

    /// <summary>A decimal literal from a curated decimal string (empty → 0).</summary>
    [SuppressMessage("Naming", "CA1720:Identifier contains type name",
        Justification = "Names the SQL DECIMAL literal it emits.")]
    public static string Decimal(object? value)
    {
        var s = value?.ToString();
        return string.IsNullOrWhiteSpace(s) ? "0" : s;
    }

    /// <summary>A boolean literal (StarRocks accepts 0/1).</summary>
    public static string Bool(object? value) =>
        value is bool b ? (b ? "1" : "0") : (Convert.ToInt64(value ?? 0L, CultureInfo.InvariantCulture) != 0 ? "1" : "0");

    /// <summary>A quoted <c>DATETIME</c> literal from epoch milliseconds (or <c>NULL</c>).</summary>
    public static string DateTimeUtc(object? epochMillis)
    {
        if (epochMillis is null)
        {
            return "NULL";
        }

        var dt = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(epochMillis, CultureInfo.InvariantCulture)).UtcDateTime;
        return "'" + dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'";
    }

    /// <summary>The integer <c>yyyymmdd</c> date key for an epoch-millisecond value.</summary>
    public static int DateKey(long epochMillis)
    {
        var d = DateTimeOffset.FromUnixTimeMilliseconds(epochMillis).UtcDateTime;
        return (d.Year * 10000) + (d.Month * 100) + d.Day;
    }
}
