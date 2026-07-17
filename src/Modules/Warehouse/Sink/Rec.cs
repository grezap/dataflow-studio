using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Avro.Generic;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>Typed readers for curated Avro <see cref="GenericRecord"/> fields.</summary>
[SuppressMessage("Naming", "CA1720:Identifier contains type name",
    Justification = "Long/Int name the Avro/CLR read type; the type-flavoured names are the clearest here.")]
public static class Rec
{
    /// <summary>Reads a required <see cref="long"/> field.</summary>
    public static long Long(GenericRecord record, string field) =>
        Convert.ToInt64(record[field], CultureInfo.InvariantCulture);

    /// <summary>Reads a required <see cref="int"/> field.</summary>
    public static int Int(GenericRecord record, string field) =>
        Convert.ToInt32(record[field], CultureInfo.InvariantCulture);

    /// <summary>Reads a string field (null → empty).</summary>
    public static string Str(GenericRecord record, string field) =>
        record[field]?.ToString() ?? string.Empty;

    /// <summary>Reads a raw field value (may be null for an Avro union).</summary>
    public static object? Raw(GenericRecord record, string field) => record[field];
}
