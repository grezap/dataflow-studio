namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// The StarRocks primitives the dimension/fact loaders depend on (execute a statement, read a scalar,
/// read a row or a lookup map). Extracting the seam keeps the loaders' SCD2/fact logic unit-testable
/// against a recording fake — the concrete <see cref="StarRocksClient"/> is the only MySQL-wire impl.
/// No EF Core (ADR-0007).
/// </summary>
public interface IStarRocksClient : IAsyncDisposable
{
    /// <summary>Opens the connection (call once before loading).</summary>
    Task OpenAsync();

    /// <summary>Executes a non-query statement (DDL/DML).</summary>
    Task ExecuteAsync(string sql);

    /// <summary>Reads a single <see cref="long"/> scalar (returns <paramref name="fallback"/> on NULL/empty).</summary>
    Task<long> ScalarLongAsync(string sql, long fallback = 0);

    /// <summary>Reads a two-column result as a <see cref="long"/>→<see cref="long"/> lookup map.</summary>
    Task<Dictionary<long, long>> LongMapAsync(string sql);

    /// <summary>Reads the first row's columns (or null if the result is empty). NULLs become null.</summary>
    Task<object?[]?> RowAsync(string sql);

    /// <summary>Reads a two-column result as a <see cref="string"/>→<see cref="long"/> lookup map.</summary>
    Task<Dictionary<string, long>> StringLongMapAsync(string sql);
}
