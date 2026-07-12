using System.Globalization;
using System.Reflection;
using DbUp;
using DbUp.Engine;
using DbUp.Engine.Output;
using MySqlConnector;

namespace DataFlowStudio.Migrations.Starrocks;

/// <summary>
/// Builds and drives the DbUp upgrade engine for the StarRocks <c>dwh</c> (Kimball star) and
/// <c>analytics</c> (serving) databases. Shared by the console entrypoint
/// (<c>nexus-cli deploy dataflow-studio</c>) and the idempotency test, so a single code path is
/// exercised everywhere. Migrations are forward-only (StarRocks DDL is not transactional and DbUp's
/// journal makes re-application a no-op).
/// </summary>
public static class StarRocksMigrationRunner
{
    /// <summary>The two databases DataFlow Studio owns on StarRocks, created before the scripts run.</summary>
    public static readonly IReadOnlyList<string> Databases = ["dwh", "analytics"];

    private const string JournalSchema = "dwh";
    private const string JournalTable = "schemaversions";

    /// <summary>
    /// Applies every pending StarRocks script. Idempotent: already-journalled scripts are skipped,
    /// so a second run performs no work. Returns the DbUp result (throws nothing — inspect
    /// <see cref="DatabaseUpgradeResult.Successful"/> / <see cref="DatabaseUpgradeResult.Error"/>).
    /// </summary>
    /// <param name="connectionString">A MySqlConnector connection string to a StarRocks FE query port (:9030).</param>
    /// <param name="replicationNum">
    /// The StarRocks replica count injected into every table's <c>replication_num</c> — 3 for the
    /// three-backend lab (the default), 1 for a single-backend test container.
    /// </param>
    /// <param name="log">Optional DbUp log sink; defaults to the console.</param>
    public static DatabaseUpgradeResult MigrateUp(string connectionString, int replicationNum = 3, IUpgradeLog? log = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentOutOfRangeException.ThrowIfLessThan(replicationNum, 1);

        // The journal table lives in `dwh`, so both owned databases must exist before DbUp runs.
        // CREATE DATABASE IF NOT EXISTS is idempotent, which keeps the whole operation replay-safe.
        EnsureDatabases(connectionString);

        var builder = DeployChanges.To
            .MySqlDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(
                Assembly.GetExecutingAssembly(),
                name => name.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            .WithVariable("replicationNum", replicationNum.ToString(CultureInfo.InvariantCulture))
            .WithExecutionTimeout(TimeSpan.FromMinutes(5));

        builder = log is null ? builder.LogToConsole() : builder.LogTo(log);

        // Swap DbUp's default MySQL journal for the StarRocks-compatible one (see StarRocksTableJournal).
        // Configure mutates the builder in place (it returns void), so this is a statement, not a link
        // in the fluent chain.
        builder.Configure(c =>
            c.Journal = new StarRocksTableJournal(() => c.ConnectionManager, () => c.Log, JournalSchema, JournalTable, replicationNum));

        return builder.Build().PerformUpgrade();
    }

    /// <summary>Creates the owned databases if they do not already exist (idempotent bootstrap).</summary>
    private static void EnsureDatabases(string connectionString)
    {
        using var connection = new MySqlConnection(connectionString);
        connection.Open();
        foreach (var database in Databases)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS `{database}`";
            command.ExecuteNonQuery();
        }
    }
}
