using System.Reflection;
using Microsoft.Data.SqlClient;

// DataFlow Studio — "dfs seed": inserts a representative order-flow dataset into OltpDb so the CDC
// pipeline has data to flow (customers, products, categories, warehouses, addresses, inventory,
// orders, order lines, transactions, shipments). Idempotent: a marker row short-circuits a re-run.
//
// Connection resolves from --connection / -c, else the DFS_SQL_CONN environment variable.

string? connectionString = ResolveConnection(args);
if (string.IsNullOrWhiteSpace(connectionString))
{
    await Console.Error.WriteLineAsync("No connection string. Pass --connection \"...\" or set DFS_SQL_CONN.")
        .ConfigureAwait(false);
    return 2;
}

var assembly = Assembly.GetExecutingAssembly();
const string resource = "DataFlowStudio.Seed.Scripts.seed-oltp.sql";
await using var stream = assembly.GetManifestResourceStream(resource)
    ?? throw new InvalidOperationException($"Embedded seed script '{resource}' not found.");
using var reader = new StreamReader(stream);
var sql = await reader.ReadToEndAsync().ConfigureAwait(false);

await using var connection = new SqlConnection(connectionString);
connection.InfoMessage += (_, e) => Console.WriteLine("  " + e.Message);
await connection.OpenAsync().ConfigureAwait(false);

await using var command = connection.CreateCommand();
command.CommandText = sql;
command.CommandTimeout = 120;
await command.ExecuteNonQueryAsync().ConfigureAwait(false);

Console.WriteLine("Seed complete.");
return 0;

static string? ResolveConnection(string[] arguments)
{
    for (int i = 0; i < arguments.Length - 1; i++)
    {
        if (arguments[i] is "--connection" or "-c")
        {
            return arguments[i + 1];
        }
    }

    return Environment.GetEnvironmentVariable("DFS_SQL_CONN");
}
