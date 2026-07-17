using System.Globalization;
using System.Text.Json;
using Avro.Generic;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// Projects a Debezium <c>after</c> image into a curated Avro <see cref="GenericRecord"/> per an
/// <see cref="EntityCurationSpec"/>. This is the pure, side-effect-free heart of curation: no Kafka,
/// no I/O — so it is exhaustively unit-testable. Decimals are carried as their string form and
/// temporal columns as epoch-millisecond longs, matching the Debezium connector settings the worker
/// relies on (<c>decimal.handling.mode=string</c>, <c>time.precision.mode=connect</c>).
/// </summary>
public static class CuratedRecordProjector
{
    /// <summary>
    /// Builds the curated record and its Kafka key from a parsed change. Throws
    /// <see cref="InvalidOperationException"/> if a non-nullable source column is absent (a mapping bug).
    /// </summary>
    /// <param name="spec">The entity mapping.</param>
    /// <param name="change">The parsed Debezium change (must have an <c>after</c> image).</param>
    /// <returns>The curated Avro record and the string message key (the spec's key field value).</returns>
    public static (GenericRecord Record, string Key) Project(EntityCurationSpec spec, in DebeziumChange change)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (!change.HasAfter)
        {
            throw new InvalidOperationException("Cannot project a change with no 'after' image.");
        }

        var after = change.After;
        var record = new GenericRecord(spec.Schema);

        foreach (var field in spec.Fields)
        {
            record.Add(field.Name, Convert(after, field));
        }

        record.Add("operation", change.Operation);
        record.Add("sourceTsMs", change.SourceTsMs);
        record.Add("curatedAtUtc", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var key = record.TryGetValue(spec.KeyField, out var keyValue) && keyValue is not null
            ? keyValue.ToString() ?? string.Empty
            : string.Empty;

        return (record, key);
    }

    private static object? Convert(JsonElement after, CuratedField field)
    {
        if (!after.TryGetProperty(field.Source, out var el) || el.ValueKind == JsonValueKind.Null)
        {
            if (field.Nullable)
            {
                return null;
            }

            throw new InvalidOperationException(
                $"Source column '{field.Source}' is missing/null but the curated field '{field.Name}' is not nullable.");
        }

        return field.Kind switch
        {
            CuratedFieldKind.Bigint => el.GetInt64(),
            CuratedFieldKind.Integer => el.GetInt32(),
            CuratedFieldKind.Text => el.GetString(),
            // Debezium string-mode decimals arrive as strings; tolerate a raw number just in case.
            CuratedFieldKind.DecimalString => el.ValueKind == JsonValueKind.String ? el.GetString() : el.GetRawText(),
            CuratedFieldKind.Boolean => el.ValueKind is JsonValueKind.True or JsonValueKind.False
                ? el.GetBoolean()
                : el.GetInt32() != 0,
            // time.precision.mode=connect emits epoch-millis longs; a numeric string is tolerated too.
            CuratedFieldKind.TimestampMillis => el.ValueKind == JsonValueKind.Number
                ? el.GetInt64()
                : long.Parse(el.GetString() ?? "0", CultureInfo.InvariantCulture),
            _ => throw new ArgumentOutOfRangeException(nameof(field), field.Kind, "Unknown curated field kind."),
        };
    }
}
