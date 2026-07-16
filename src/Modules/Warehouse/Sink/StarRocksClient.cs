using System.Globalization;
using MySqlConnector;

namespace DataFlowStudio.Modules.Warehouse.Sink;

/// <summary>
/// A thin StarRocks client over the MySQL wire protocol (FE query port :9030). Holds one open
/// connection for the loader run and exposes the few primitives the dimension/fact loaders need:
/// execute a statement, read a scalar, and read a key→value lookup map. No EF Core (ADR-0007).
/// </summary>
public sealed class StarRocksClient(string connectionString) : IAsyncDisposable
{
    private MySqlConnection? _connection;

    /// <summary>Opens the connection (call once before loading).</summary>
    public async Task OpenAsync()
    {
        _connection = new MySqlConnection(connectionString);
        await _connection.OpenAsync().ConfigureAwait(false);
    }

    /// <summary>Executes a non-query statement (DDL/DML).</summary>
    public async Task ExecuteAsync(string sql)
    {
        await using var command = Connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <summary>Reads a single <see cref="long"/> scalar (returns <paramref name="fallback"/> on NULL/empty).</summary>
    public async Task<long> ScalarLongAsync(string sql, long fallback = 0)
    {
        await using var command = Connection.CreateCommand();
        command.CommandText = sql;
        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return result is null || result is DBNull ? fallback : Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    /// <summary>Reads a two-column result as a <see cref="long"/>→<see cref="long"/> lookup map.</summary>
    public async Task<Dictionary<long, long>> LongMapAsync(string sql)
    {
        var map = new Dictionary<long, long>();
        await using var command = Connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            map[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        return map;
    }

    /// <summary>Reads the first row's columns (or null if the result is empty). NULLs become null.</summary>
    public async Task<object?[]?> RowAsync(string sql)
    {
        await using var command = Connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        if (!await reader.ReadAsync().ConfigureAwait(false))
        {
            return null;
        }

        var row = new object?[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            row[i] = await reader.IsDBNullAsync(i).ConfigureAwait(false) ? null : reader.GetValue(i);
        }

        return row;
    }

    /// <summary>Reads a two-column result as a <see cref="string"/>→<see cref="long"/> lookup map.</summary>
    public async Task<Dictionary<string, long>> StringLongMapAsync(string sql)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var command = Connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            map[reader.GetString(0)] = reader.GetInt64(1);
        }

        return map;
    }

    private MySqlConnection Connection =>
        _connection ?? throw new InvalidOperationException("StarRocksClient is not open; call OpenAsync first.");

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
