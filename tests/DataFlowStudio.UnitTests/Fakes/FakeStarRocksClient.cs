using DataFlowStudio.Modules.Warehouse.Sink;

namespace DataFlowStudio.UnitTests.Fakes;

/// <summary>
/// A recording <see cref="IStarRocksClient"/> for the DWH loader tests: it captures every executed
/// statement in order and answers reads from configurable delegates, so the SCD2 / fact logic can be
/// asserted without a live StarRocks. The delegates default to "empty database" (no current versions,
/// zero surrogate keys, empty lookups).
/// </summary>
internal sealed class FakeStarRocksClient : IStarRocksClient
{
    /// <summary>Every <see cref="ExecuteAsync"/> statement, in the order the loaders issued them.</summary>
    public List<string> Executed { get; } = [];

    /// <summary>Answers <see cref="ScalarLongAsync"/> (e.g. <c>MAX(sk)</c>). Default: 0.</summary>
    public Func<string, long> OnScalarLong { get; set; } = _ => 0;

    /// <summary>Answers <see cref="RowAsync"/> (the current SCD2 version, or null). Default: null.</summary>
    public Func<string, object?[]?> OnRow { get; set; } = _ => null;

    /// <summary>Answers <see cref="LongMapAsync"/> (surrogate-key lookups). Default: empty.</summary>
    public Func<string, Dictionary<long, long>> OnLongMap { get; set; } = _ => [];

    /// <summary>Answers <see cref="StringLongMapAsync"/> (existing carriers). Default: empty.</summary>
    public Func<string, Dictionary<string, long>> OnStringLongMap { get; set; } = _ => [];

    /// <summary>Set to throw from <see cref="ExecuteAsync"/> when the statement contains this fragment.</summary>
    public string? ThrowOnExecuteContaining { get; set; }

    public bool Opened { get; private set; }

    public bool Disposed { get; private set; }

    public Task OpenAsync()
    {
        Opened = true;
        return Task.CompletedTask;
    }

    public Task ExecuteAsync(string sql)
    {
        if (ThrowOnExecuteContaining is { } fragment && sql.Contains(fragment, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"fake execute failure on: {fragment}");
        }

        Executed.Add(sql);
        return Task.CompletedTask;
    }

    public Task<long> ScalarLongAsync(string sql, long fallback = 0) => Task.FromResult(OnScalarLong(sql));

    public Task<Dictionary<long, long>> LongMapAsync(string sql) => Task.FromResult(OnLongMap(sql));

    public Task<object?[]?> RowAsync(string sql) => Task.FromResult(OnRow(sql));

    public Task<Dictionary<string, long>> StringLongMapAsync(string sql) => Task.FromResult(OnStringLongMap(sql));

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}
