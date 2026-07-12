using ClickHouse.Client.ADO;
using DataFlowStudio.Migrations.Clickhouse;

// Deploy-time migration tool for the ClickHouse analytics telemetry schema (wired into
// `nexus-cli deploy dataflow-studio`).
//
//   DataFlowStudio.Migrations.Clickhouse [up] [--single-node] [--connection "<cs>"]
//
// The connection string resolves from --connection / -c, else DFS_CLICKHOUSE_CONNECTION. It targets
// a ClickHouse HTTP(S) interface, e.g.
//   "Host=192.168.70.44;Port=8443;Protocol=https;Username=admin;Password=***;Database=default"
// The lab is a replicated nexus_analytics cluster (default profile); pass --single-node for a lone
// container. Migrations are forward-only; re-running is a safe no-op (journal table).

string command = args.Length > 0 && !args[0].StartsWith('-') ? args[0].ToLowerInvariant() : "up";
string? connectionString = ResolveConnection(args);
bool singleNode = args.Contains("--single-node", StringComparer.OrdinalIgnoreCase);

if (string.IsNullOrWhiteSpace(connectionString))
{
    await Console.Error.WriteLineAsync(
        "No connection string. Pass --connection \"...\" or set DFS_CLICKHOUSE_CONNECTION.").ConfigureAwait(false);
    return 2;
}

if (command != "up")
{
    await Console.Error.WriteLineAsync(
        $"Unknown command '{command}'. ClickHouse migrations are forward-only; expected: up.").ConfigureAwait(false);
    return 2;
}

var profile = singleNode ? ClickHouseMigrationProfile.SingleNode() : ClickHouseMigrationProfile.Lab();
var result = ClickHouseMigrationRunner.MigrateUp(() => new ClickHouseConnection(connectionString), profile);
if (result.Successful)
{
    Console.WriteLine($"ClickHouse migration succeeded ({result.ScriptsExecuted.Count} script(s) applied).");
    return 0;
}

await Console.Error.WriteLineAsync($"ClickHouse migration failed: {result.Error}").ConfigureAwait(false);
return 1;

static string? ResolveConnection(string[] arguments)
{
    for (int i = 0; i < arguments.Length - 1; i++)
    {
        if (arguments[i] is "--connection" or "-c")
        {
            return arguments[i + 1];
        }
    }

    return Environment.GetEnvironmentVariable("DFS_CLICKHOUSE_CONNECTION");
}
