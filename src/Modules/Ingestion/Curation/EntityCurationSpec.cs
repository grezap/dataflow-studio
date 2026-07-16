using System.Text;
using Avro;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// The declarative mapping for one order-flow entity: which Debezium raw topic it is captured on,
/// which curated topic it is re-published to, and how each field projects from the raw <c>after</c>
/// image into the clean curated Avro record. The catalog (<see cref="CurationCatalog"/>) is a list
/// of these, so adding an entity is data, not code — one spec, no new worker.
/// </summary>
public sealed class EntityCurationSpec
{
    /// <summary>The Avro namespace every curated domain record shares.</summary>
    public const string CuratedNamespace = "com.nexusplatform.dataflowstudio.curated";

    private readonly Lazy<RecordSchema> _schema;

    /// <summary>Creates a spec and prepares its (lazily-parsed) curated Avro schema.</summary>
    /// <param name="entity">Human-readable entity name (e.g. <c>customers</c>), used in logs/metrics.</param>
    /// <param name="rawTopic">The Debezium raw topic (e.g. <c>oltp.OltpDb.dbo.Customers</c>).</param>
    /// <param name="curatedTopic">The curated topic (e.g. <c>dfs.customers.changed.v1</c>).</param>
    /// <param name="recordName">The curated Avro record name (e.g. <c>CustomerChanged</c>).</param>
    /// <param name="keyField">The curated field whose value is the Kafka message key (natural/business key).</param>
    /// <param name="fields">The business fields; the envelope fields (operation/sourceTsMs/curatedAtUtc) are appended automatically.</param>
    public EntityCurationSpec(
        string entity,
        string rawTopic,
        string curatedTopic,
        string recordName,
        string keyField,
        IReadOnlyList<CuratedField> fields)
    {
        Entity = entity;
        RawTopic = rawTopic;
        CuratedTopic = curatedTopic;
        RecordName = recordName;
        KeyField = keyField;
        Fields = fields;
        Subject = curatedTopic + "-value";
        _schema = new Lazy<RecordSchema>(() => (RecordSchema)Avro.Schema.Parse(BuildSchemaJson()));
    }

    /// <summary>Human-readable entity name.</summary>
    public string Entity { get; }

    /// <summary>The Debezium raw CDC topic this entity is captured on.</summary>
    public string RawTopic { get; }

    /// <summary>The curated topic this entity is re-published to.</summary>
    public string CuratedTopic { get; }

    /// <summary>The curated Avro record name.</summary>
    public string RecordName { get; }

    /// <summary>The curated field used as the Kafka message key.</summary>
    public string KeyField { get; }

    /// <summary>The business fields (envelope fields are added to the schema automatically).</summary>
    public IReadOnlyList<CuratedField> Fields { get; }

    /// <summary>The Schema Registry subject for the curated value (<c>&lt;topic&gt;-value</c>).</summary>
    public string Subject { get; }

    /// <summary>The parsed curated Avro schema (business fields + envelope).</summary>
    public RecordSchema Schema => _schema.Value;

    /// <summary>The curated Avro schema as JSON (business fields + the three envelope fields).</summary>
    public string BuildSchemaJson()
    {
        var sb = new StringBuilder();
        sb.Append("{\"type\":\"record\",\"name\":\"").Append(RecordName)
          .Append("\",\"namespace\":\"").Append(CuratedNamespace).Append("\",\"fields\":[");

        foreach (var f in Fields)
        {
            // Every field carries a default so the schema evolves compatibly (BACKWARD): adding a
            // field later is a compatible change. Nullable fields default to null; others to a
            // type-appropriate zero value.
            var type = f.Nullable ? $"[\"null\",\"{f.AvroType}\"]" : $"\"{f.AvroType}\"";
            var defaultJson = f.Nullable ? "null" : DefaultFor(f.Kind);
            sb.Append("{\"name\":\"").Append(f.Name).Append("\",\"type\":").Append(type)
              .Append(",\"default\":").Append(defaultJson).Append("},");
        }

        // Envelope fields present on every curated event.
        sb.Append("{\"name\":\"operation\",\"type\":\"string\"},");
        sb.Append("{\"name\":\"sourceTsMs\",\"type\":\"long\"},");
        sb.Append("{\"name\":\"curatedAtUtc\",\"type\":\"long\"}");
        sb.Append("]}");
        return sb.ToString();
    }

    private static string DefaultFor(CuratedFieldKind kind) => kind switch
    {
        CuratedFieldKind.Text or CuratedFieldKind.DecimalString => "\"\"",
        CuratedFieldKind.Boolean => "false",
        _ => "0",   // Bigint, Integer, TimestampMillis
    };
}
