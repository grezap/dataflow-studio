using System.Diagnostics.CodeAnalysis;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// The Avro shape of a curated field, chosen so the projection from Debezium's raw JSON is
/// unambiguous. Decimals arrive as strings (Debezium <c>decimal.handling.mode=string</c>) and
/// temporal columns arrive as epoch-millisecond longs (Debezium <c>time.precision.mode=connect</c>),
/// so both map to a stable, self-describing curated type rather than a source-specific encoding.
/// </summary>
[SuppressMessage("Naming", "CA1720:Identifier contains type name",
    Justification = "These members name curated Avro/SQL kinds (Bigint/Integer/Text); the SQL-flavoured names are the clearest domain vocabulary.")]
public enum CuratedFieldKind
{
    /// <summary>64-bit integer (<c>bigint</c>, IDENTITY keys).</summary>
    Bigint,

    /// <summary>32-bit integer (<c>int</c>/<c>smallint</c>/<c>tinyint</c>).</summary>
    Integer,

    /// <summary>UTF-8 string (<c>varchar</c>/<c>nvarchar</c>/<c>char</c>).</summary>
    Text,

    /// <summary>A fixed-point money value carried losslessly as its decimal string form.</summary>
    DecimalString,

    /// <summary>Boolean (<c>bit</c>).</summary>
    Boolean,

    /// <summary>A point in time as epoch milliseconds (<c>datetime2</c>/<c>date</c>).</summary>
    TimestampMillis,
}

/// <summary>
/// One field of a curated event: its curated (camelCase) name, the source column it projects from in
/// Debezium's <c>after</c> image, its <see cref="CuratedFieldKind"/>, and whether it is nullable
/// (nullable fields become an Avro <c>["null", T]</c> union).
/// </summary>
/// <param name="Name">The curated field name (camelCase, the wire contract).</param>
/// <param name="Source">The source column name in Debezium's <c>after</c> image (PascalCase, as in OltpDb).</param>
/// <param name="Kind">The curated Avro type.</param>
/// <param name="Nullable">True if the source column is nullable (emits an Avro null union).</param>
public sealed record CuratedField(string Name, string Source, CuratedFieldKind Kind, bool Nullable = false)
{
    /// <summary>The Avro primitive type name for this field's <see cref="Kind"/>.</summary>
    public string AvroType => Kind switch
    {
        CuratedFieldKind.Bigint => "long",
        CuratedFieldKind.Integer => "int",
        CuratedFieldKind.Text => "string",
        CuratedFieldKind.DecimalString => "string",
        CuratedFieldKind.Boolean => "boolean",
        CuratedFieldKind.TimestampMillis => "long",
        _ => throw new ArgumentOutOfRangeException(nameof(Kind), Kind, "Unknown curated field kind."),
    };
}
