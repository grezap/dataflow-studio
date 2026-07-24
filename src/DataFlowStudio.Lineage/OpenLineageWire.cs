using System.Text.Json.Serialization;

namespace DataFlowStudio.Lineage;

// The minimal OpenLineage RunEvent wire shape Marquez accepts at POST /api/v1/lineage (proven by the
// platform-tools marquez-lineage-demo). Serialized with the source-generated context below (camelCase),
// so there is no reflection-based serialization on the wire.

internal sealed record RunEvent(
    string EventType,
    string EventTime,
    string Producer,
    RunRef Run,
    JobRef Job,
    IReadOnlyList<DatasetRef> Inputs,
    IReadOnlyList<DatasetRef> Outputs);

internal sealed record RunRef(string RunId);

internal sealed record JobRef(string Namespace, string Name);

internal sealed record DatasetRef(string Namespace, string Name);

/// <summary>Source-generated JSON context for the OpenLineage wire records (camelCase; trim/AOT-safe).</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(RunEvent))]
internal sealed partial class LineageJsonContext : JsonSerializerContext;
