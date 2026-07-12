using DataFlowStudio.Migrations.Starrocks;

// Deploy-time migration tool for the StarRocks dwh + analytics schema (wired into
// `nexus-cli deploy dataflow-studio`).
//
//   DataFlowStudio.Migrations.Starrocks [up] [--connection "<cs>"]
//
// The connection string resolves from --connection / -c, else the DFS_STARROCKS_CONNECTION env var.
// It must target a StarRocks FE query port (:9030) and speak the MySQL wire protocol, e.g.
//   "Server=192.168.70.31;Port=9030;User ID=root;Password=***;SslMode=None"
// Migrations are forward-only; re-running is a safe no-op (DbUp journal).

string command = args.Length > 0 && !args[0].StartsWith('-') ? args[0].ToLowerInvariant() : "up";
string? connectionString = ResolveConnection(args);

if (string.IsNullOrWhiteSpace(connectionString))
{
    await Console.Error.WriteLineAsync(
        "No connection string. Pass --connection \"...\" or set DFS_STARROCKS_CONNECTION.").ConfigureAwait(false);
    return 2;
}

if (command != "up")
{
    await Console.Error.WriteLineAsync(
        $"Unknown command '{command}'. StarRocks migrations are forward-only; expected: up.").ConfigureAwait(false);
    return 2;
}

var result = StarRocksMigrationRunner.MigrateUp(connectionString);
if (result.Successful)
{
    Console.WriteLine("StarRocks migration succeeded.");
    return 0;
}

await Console.Error.WriteLineAsync($"StarRocks migration failed: {result.Error}").ConfigureAwait(false);
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

    return Environment.GetEnvironmentVariable("DFS_STARROCKS_CONNECTION");
}
