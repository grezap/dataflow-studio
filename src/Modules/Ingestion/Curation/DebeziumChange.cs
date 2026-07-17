using System.Text.Json;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// A parsed Debezium change envelope. The Connect worker emits JSON (<c>schemas.enable=false</c>),
/// so the payload sits either at the root or under a <c>payload</c> property; this normalizes both.
/// Only changes with an <c>after</c> image (snapshot / insert / update) carry a projectable row —
/// hard deletes (<c>op = d</c>, null <c>after</c>) do not occur in OltpDb (it soft-deletes via
/// <c>is_deleted</c>), so the worker skips them.
/// </summary>
public readonly struct DebeziumChange
{
    private DebeziumChange(string operation, JsonElement after, bool hasAfter, long sourceTsMs)
    {
        Operation = operation;
        After = after;
        HasAfter = hasAfter;
        SourceTsMs = sourceTsMs;
    }

    /// <summary>Normalized operation: <c>snapshot</c> / <c>insert</c> / <c>update</c> / <c>delete</c>.</summary>
    public string Operation { get; }

    /// <summary>The <c>after</c> image (the current row state); valid only when <see cref="HasAfter"/> is true.</summary>
    public JsonElement After { get; }

    /// <summary>True when an <c>after</c> image is present (snapshot / insert / update).</summary>
    public bool HasAfter { get; }

    /// <summary>The source-commit time in epoch milliseconds (Debezium <c>source.ts_ms</c>).</summary>
    public long SourceTsMs { get; }

    /// <summary>Parses a raw Debezium message value into a <see cref="DebeziumChange"/>.</summary>
    public static DebeziumChange Parse(string rawJson)
    {
        using var doc = JsonDocument.Parse(rawJson);
        var payload = doc.RootElement.TryGetProperty("payload", out var p) ? p : doc.RootElement;

        var op = payload.TryGetProperty("op", out var opEl) ? opEl.GetString() ?? "?" : "?";
        var operation = op switch
        {
            "r" => "snapshot",
            "c" => "insert",
            "u" => "update",
            "d" => "delete",
            _ => op,
        };

        long tsMs = payload.TryGetProperty("source", out var src) && src.TryGetProperty("ts_ms", out var ts)
            ? ts.GetInt64()
            : 0;

        bool hasAfter = payload.TryGetProperty("after", out var after) && after.ValueKind != JsonValueKind.Null;

        // Clone the after element so it outlives the disposed JsonDocument.
        return new DebeziumChange(operation, hasAfter ? after.Clone() : default, hasAfter, tsMs);
    }
}
