using Dapper;
using DataFlowStudio.Migrations.Oltp;
using Microsoft.Data.SqlClient;
using Shouldly;
using Testcontainers.MsSql;
using Xunit;

namespace DataFlowStudio.Migrations.Tests;

/// <summary>
/// MASTER-PLAN enhancement E1 acceptance gate: FluentMigrator <c>up → down → up</c> must succeed
/// on a fresh SQL Server. The rollback proves every migration's <c>Down()</c> reverses its
/// <c>Up()</c> (including the two system-versioned temporal tables), and the second <c>up</c>
/// proves the schema rebuilds cleanly from zero.
/// <para>
/// Locally this runs against SQL Server LocalDB — set <c>DFS_OLTP_TEST_CONNECTION</c> to the
/// LocalDB master connection string. In CI (Docker available) it spins a throwaway SQL Server
/// container via Testcontainers.
/// </para>
/// </summary>
public sealed class OltpMigrationUpDownUpTests : IAsyncLifetime
{
    private const string TestDatabase = "DataFlowStudio_E1";

    private static readonly IReadOnlyList<(string Schema, string Name)> AllExpected =
        [.. OltpTables.Business, .. OltpTables.TemporalHistory];

    private MsSqlContainer? _container;
    private string _connectionString = string.Empty;

    public async Task InitializeAsync()
    {
        SqlConnection.ClearAllPools();

        string masterConnectionString;
        var external = Environment.GetEnvironmentVariable("DFS_OLTP_TEST_CONNECTION");
        if (!string.IsNullOrWhiteSpace(external))
        {
            masterConnectionString = external;
        }
        else
        {
            // Testcontainers 4.13 requires an explicit image (the parameterless builder is obsolete).
            _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
            await _container.StartAsync();
            masterConnectionString = _container.GetConnectionString();
        }

        // Drop + recreate a dedicated database so the E1 gate always starts from a truly fresh schema.
        var masterBuilder = new SqlConnectionStringBuilder(masterConnectionString) { InitialCatalog = "master" };
        await using (var conn = new SqlConnection(masterBuilder.ConnectionString))
        {
            await conn.OpenAsync();
            await conn.ExecuteAsync($"""
                IF DB_ID('{TestDatabase}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{TestDatabase}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{TestDatabase}];
                END
                CREATE DATABASE [{TestDatabase}];
                """);
        }

        _connectionString = new SqlConnectionStringBuilder(masterConnectionString)
        {
            InitialCatalog = TestDatabase,
        }.ConnectionString;
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }

    [Fact]
    public async Task Up_down_up_leaves_the_full_oltp_schema_present()
    {
        OltpMigrationRunner.MigrateUp(_connectionString);
        (await CountPresentAsync()).ShouldBe(AllExpected.Count, "every table exists after the first up");

        OltpMigrationRunner.MigrateDownToZero(_connectionString);
        (await CountPresentAsync()).ShouldBe(0, "down() reverses every migration, dropping the whole schema");

        OltpMigrationRunner.MigrateUp(_connectionString);
        (await CountPresentAsync()).ShouldBe(AllExpected.Count, "E1 gate: up succeeds again after a full rollback");
    }

    private async Task<int> CountPresentAsync()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        int present = 0;
        foreach (var (schema, name) in AllExpected)
        {
            present += await conn.ExecuteScalarAsync<int>(
                """
                SELECT COUNT(*)
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = @schema AND t.name = @name
                """,
                new { schema, name });
        }

        return present;
    }
}
