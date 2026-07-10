using DataFlowStudio.Migrations.Oltp;

// Deploy-time migration tool for OltpDb (wired into `nexus-cli deploy dataflow-studio`).
//
//   DataFlowStudio.Migrations.Oltp <up|down|rollback-all> [--connection "<cs>"]
//
// The connection string resolves from --connection / -c, else the DFS_OLTP_CONNECTION env var.

string command = args.Length > 0 ? args[0].ToLowerInvariant() : "up";
string? connectionString = ResolveConnection(args);

if (string.IsNullOrWhiteSpace(connectionString))
{
    await Console.Error.WriteLineAsync(
        "No connection string. Pass --connection \"...\" or set DFS_OLTP_CONNECTION.").ConfigureAwait(false);
    return 2;
}

switch (command)
{
    case "up":
        OltpMigrationRunner.MigrateUp(connectionString);
        return 0;

    case "down":
    case "rollback-all":
        OltpMigrationRunner.MigrateDownToZero(connectionString);
        return 0;

    default:
        await Console.Error.WriteLineAsync(
            $"Unknown command '{command}'. Expected: up | down | rollback-all.").ConfigureAwait(false);
        return 2;
}

static string? ResolveConnection(string[] arguments)
{
    for (int i = 0; i < arguments.Length - 1; i++)
    {
        if (arguments[i] is "--connection" or "-c")
        {
            return arguments[i + 1];
        }
    }

    return Environment.GetEnvironmentVariable("DFS_OLTP_CONNECTION");
}
