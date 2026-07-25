using Avro;
using Avro.Generic;

namespace DataFlowStudio.UnitTests.Fakes;

/// <summary>
/// Builds curated-shaped <see cref="GenericRecord"/>s for the DWH loader tests without depending on the
/// full curation catalog schemas — each test declares just the fields its loader reads.
/// </summary>
internal static class AvroRecord
{
    /// <summary>Builds a <see cref="GenericRecord"/> from <paramref name="fields"/> (name, Avro primitive type, value).</summary>
    public static GenericRecord Of(string recordName, params (string Name, string Type, object? Value)[] fields)
    {
        var fieldJson = string.Join(",", fields.Select(f => $"{{\"name\":\"{f.Name}\",\"type\":\"{f.Type}\"}}"));
        var schema = (RecordSchema)Schema.Parse(
            $"{{\"type\":\"record\",\"name\":\"{recordName}\",\"fields\":[{fieldJson}]}}");
        var record = new GenericRecord(schema);
        foreach (var f in fields)
        {
            record.Add(f.Name, f.Value);
        }

        return record;
    }
}
