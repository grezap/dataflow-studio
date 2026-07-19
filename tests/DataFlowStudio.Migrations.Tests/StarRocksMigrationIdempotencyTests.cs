using DataFlowStudio.Migrations.Starrocks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MySqlConnector;
using Shouldly;
using Xunit;

namespace DataFlowStudio.Migrations.Tests;

/// <summary>
/// MASTER-PLAN enhancement E1 acceptance gate for the StarRocks sink: DbUp must apply the whole
/// <c>dwh</c> star schema on a fresh target, and a second run must be a no-op (idempotent,
/// forward-only). StarRocks migrations are not reversible, so the gate is <c>apply → re-apply</c>
/// rather than the OltpDb <c>up → down → up</c>.
/// <para>
/// Runs against a throwaway <c>starrocks/allin1-ubuntu</c> container (a single frontend + single
/// backend, so <c>replication_num = 1</c>). Set <c>DFS_STARROCKS_TEST_CONNECTION</c> to a MySQL-wire
/// connection string to target an external StarRocks instead.
/// </para>
/// </summary>
public sealed class StarRocksMigrationIdempotencyTests : IAsyncLifetime
{
    // Pin to the lab's StarRocks line (3.5.x) so the container validates the same DDL surface.
    private const string AllInOneImage = "starrocks/allin1-ubuntu:3.5.17";

    // allin1 has one backend, so tables can request only a single replica.
    private const int ContainerReplicationNum = 1;

    private IContainer? _container;
    private string _connectionString = string.Empty;

    public async Task InitializeAsync()
    {
        var external = Environment.GetEnvironmentVariable("DFS_STARROCKS_TEST_CONNECTION");
        if (!string.IsNullOrWhiteSpace(external))
        {
            _connectionString = external;
            return;
        }

        _container = new ContainerBuilder(AllInOneImage)
            .WithPortBinding(9030, assignRandomHostPort: true)   // FE MySQL query port
            .WithPortBinding(8030, assignRandomHostPort: true)   // FE HTTP port
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged("Enjoy the journey to StarRocks", o => o.WithTimeout(TimeSpan.FromMinutes(5))))
            .Build();
        await _container.StartAsync();

        _connectionString = new MySqlConnectionStringBuilder
        {
            Server = _container.Hostname,
            Port = (uint)_container.GetMappedPublicPort(9030),
            UserID = "root",
            Password = string.Empty,
            SslMode = MySqlSslMode.None,
            AllowPublicKeyRetrieval = true,
        }.ConnectionString;

        // The backend registers with the frontend a few seconds after the "Enjoy the journey" banner;
        // poll until a trivial query succeeds so the first CREATE TABLE does not race BE registration.
        await WaitForQueryableAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }

    [Fact]
    public void Apply_is_idempotent_and_lands_the_full_dwh_star()
    {
        int replicationNum = _container is null ? 3 : ContainerReplicationNum;

        var first = StarRocksMigrationRunner.MigrateUp(_connectionString, replicationNum);
        first.Successful.ShouldBeTrue(first.Error?.ToString());
        CountPresent().ShouldBe(StarRocksTables.All.Count, "every dwh table exists after the first apply");

        var second = StarRocksMigrationRunner.MigrateUp(_connectionString, replicationNum);
        second.Successful.ShouldBeTrue(second.Error?.ToString());
        second.Scripts.ShouldBeEmpty("re-applying a fully-migrated schema is a no-op");
        CountPresent().ShouldBe(StarRocksTables.All.Count, "the schema is unchanged after the second apply");
    }

    private int CountPresent()
    {
        using var connection = new MySqlConnection(_connectionString);
        connection.Open();

        int present = 0;
        foreach (var (database, name) in StarRocksTables.All)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = @name";
            command.Parameters.AddWithValue("@db", database);
            command.Parameters.AddWithValue("@name", name);
            present += Convert.ToInt32(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
        }

        return present;
    }

    private async Task WaitForQueryableAsync()
    {
        var deadline = DateTime.UtcNow.AddMinutes(2);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var connection = new MySqlConnection(_connectionString);
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                await command.ExecuteScalarAsync();
                return;
            }
            catch (MySqlException)
            {
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        }
    }
}
