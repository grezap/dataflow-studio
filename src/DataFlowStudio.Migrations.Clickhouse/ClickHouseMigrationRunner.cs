using System.Reflection;
using System.Text.RegularExpressions;
using ClickHouse.Client.ADO;

namespace DataFlowStudio.Migrations.Clickhouse;

/// <summary>The outcome of a ClickHouse migration run.</summary>
/// <param name="Successful">True when every pending script applied without error.</param>
/// <param name="Error">The failure message when <paramref name="Successful"/> is false; otherwise null.</param>
/// <param name="ScriptsExecuted">The names of the scripts applied this run (empty on a no-op re-run).</param>
public sealed record ClickHouseMigrationResult(
    bool Successful,
    string? Error,
    IReadOnlyList<string> ScriptsExecuted);

/// <summary>
/// A purpose-built, DbUp-pattern migration runner for the ClickHouse <c>analytics</c> telemetry
/// database. DbUp has no ClickHouse provider, and ClickHouse's lack of transactions plus its
/// <c>ON CLUSTER</c> DDL make DbUp's generic executor unsuitable — so this reimplements only the
/// pieces that matter: a journal table of applied script names, ordered forward-only embedded
/// scripts, <c>$variable$</c> substitution (see <see cref="ClickHouseMigrationProfile"/>), and a
/// skip-if-already-applied loop that makes a second run a no-op. Shared by the console entrypoint
/// and the idempotency test (ADR-0005).
/// </summary>
public static partial class ClickHouseMigrationRunner
{
    private const string Database = "analytics";
    private const string JournalTable = "dfs_schema_versions";

    [GeneratedRegex(@"\$(\w+)\$")]
    private static partial Regex VariablePattern();

    /// <summary>
    /// Applies every pending script against the ClickHouse connection the factory yields, using the
    /// given <paramref name="profile"/> to adapt the DDL to the target topology. Never throws —
    /// inspect <see cref="ClickHouseMigrationResult.Successful"/>. Idempotent: journalled scripts are
    /// skipped, so a re-run executes nothing.
    /// </summary>
    /// <param name="connectionFactory">Yields a fresh, un-opened ClickHouse connection to the target node.</param>
    /// <param name="profile">The lab or single-node substitution profile.</param>
    /// <param name="log">Optional progress sink; defaults to <see cref="Console.Out"/>.</param>
    public static ClickHouseMigrationResult MigrateUp(
        Func<ClickHouseConnection> connectionFactory,
        ClickHouseMigrationProfile profile,
        TextWriter? log = null)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);
        ArgumentNullException.ThrowIfNull(profile);
        log ??= Console.Out;

        var executed = new List<string>();
        try
        {
            using var connection = connectionFactory();
            connection.Open();

            EnsureJournal(connection, profile.Variables["onCluster"]);
            var alreadyApplied = GetExecutedScripts(connection);

            foreach (var script in DiscoverScripts())
            {
                if (alreadyApplied.Contains(script.Name))
                {
                    log.WriteLine($"  - skip {script.Name} (already applied)");
                    continue;
                }

                log.WriteLine($"  + apply {script.Name}");
                var sql = Substitute(script.Contents, profile.Variables);
                foreach (var statement in SplitStatements(sql))
                {
                    Execute(connection, statement);
                }

                RecordApplied(connection, script.Name);
                executed.Add(script.Name);
            }

            return new ClickHouseMigrationResult(true, null, executed);
        }
        catch (Exception ex)
        {
            return new ClickHouseMigrationResult(false, ex.Message, executed);
        }
    }

    /// <summary>Creates the <c>analytics</c> database (cluster-wide) and the local journal table.</summary>
    private static void EnsureJournal(ClickHouseConnection connection, string onCluster)
    {
        // The database is created ON CLUSTER (lab) so the ON CLUSTER table DDL that follows can
        // materialize on every node. The journal itself is a plain local table on the control node
        // we always connect to — migrations are operated from one node, so no replication is needed.
        Execute(connection, $"CREATE DATABASE IF NOT EXISTS {Database}{onCluster}");
        Execute(connection,
            $"""
            CREATE TABLE IF NOT EXISTS {Database}.{JournalTable} (
                script_name String,
                applied_utc DateTime64(3) DEFAULT now64(3)
            ) ENGINE = MergeTree ORDER BY script_name
            """);
    }

    /// <summary>Reads the set of script names already recorded in the journal.</summary>
    private static HashSet<string> GetExecutedScripts(ClickHouseConnection connection)
    {
        var applied = new HashSet<string>(StringComparer.Ordinal);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT DISTINCT script_name FROM {Database}.{JournalTable}";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            applied.Add(reader.GetString(0));
        }

        return applied;
    }

    /// <summary>Records a script as applied (script names are literal file names, quotes escaped defensively).</summary>
    private static void RecordApplied(ClickHouseConnection connection, string scriptName) =>
        Execute(connection,
            $"INSERT INTO {Database}.{JournalTable} (script_name) VALUES ('{scriptName.Replace("'", "''", StringComparison.Ordinal)}')");

    /// <summary>Executes a single statement with no result set.</summary>
    private static void Execute(ClickHouseConnection connection, string statement)
    {
        using var command = connection.CreateCommand();
        command.CommandText = statement;
        command.ExecuteNonQuery();
    }

    /// <summary>The embedded forward-only scripts, ordered by file name (Script0001, Script0002, …).</summary>
    private static List<EmbeddedScript> DiscoverScripts()
    {
        var assembly = Assembly.GetExecutingAssembly();
        const string marker = ".Scripts.";
        return assembly.GetManifestResourceNames()
            .Where(name => name.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            .OrderBy(name => name, StringComparer.Ordinal)
            .Select(name => new EmbeddedScript(
                name[(name.IndexOf(marker, StringComparison.Ordinal) + marker.Length)..],
                ReadResource(assembly, name)))
            .ToList();
    }

    private static string ReadResource(Assembly assembly, string resourceName)
    {
        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded script '{resourceName}' not found.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <summary>Replaces every <c>$name$</c> token; an unknown token is a fail-fast authoring error.</summary>
    private static string Substitute(string sql, IReadOnlyDictionary<string, string> variables) =>
        VariablePattern().Replace(sql, match =>
            variables.TryGetValue(match.Groups[1].Value, out var value)
                ? value
                : throw new InvalidOperationException($"Unknown migration variable ${match.Groups[1].Value}$"));

    /// <summary>
    /// Splits a multi-statement script on <c>;</c> after stripping <c>--</c> comment lines. The
    /// analytics DDL contains no semicolons inside identifiers or literals, so a plain split is safe.
    /// </summary>
    private static IEnumerable<string> SplitStatements(string sql)
    {
        var withoutComments = string.Join(
            '\n',
            sql.Split('\n').Where(line => !line.TrimStart().StartsWith("--", StringComparison.Ordinal)));

        foreach (var part in withoutComments.Split(';'))
        {
            if (!string.IsNullOrWhiteSpace(part))
            {
                yield return part.Trim();
            }
        }
    }

    private readonly record struct EmbeddedScript(string Name, string Contents);
}
