using System.Globalization;
using System.Text;
using ClickHouse.Client.ADO;
using DataFlowStudio.Migrations.Clickhouse;
using FluentAssertions;
using Testcontainers.ClickHouse;
using Xunit;

namespace DataFlowStudio.Migrations.Tests;

/// <summary>
/// MASTER-PLAN enhancement E1 acceptance gate for the ClickHouse sink: the migration runner must
/// create the whole <c>analytics</c> telemetry schema on a fresh target, and a second run must apply
/// nothing (idempotent, forward-only). Runs against a throwaway single-node ClickHouse container.
/// <para>
/// The container has no ClickHouse Keeper, so it uses the <see cref="ClickHouseMigrationProfile.SingleNode"/>
/// profile (plain MergeTree, no <c>ON CLUSTER</c>). A one-node <c>nexus_analytics</c> cluster is
/// injected via a mounted config so the <c>Distributed</c> table still resolves — proving the same
/// script set the replicated lab runs is structurally valid. Set <c>DFS_CLICKHOUSE_TEST_CONNECTION</c>
/// to target an external ClickHouse instead.
/// </para>
/// </summary>
public sealed class ClickHouseMigrationIdempotencyTests : IAsyncLifetime
{
    private const string ClickHouseImage = "clickhouse/clickhouse-server:24.8";

    private const string ClusterConfigXml =
        """
        <clickhouse>
            <remote_servers>
                <nexus_analytics>
                    <shard>
                        <internal_replication>true</internal_replication>
                        <replica><host>localhost</host><port>9000</port></replica>
                    </shard>
                </nexus_analytics>
            </remote_servers>
        </clickhouse>
        """;

    private ClickHouseContainer? _container;
    private string _connectionString = string.Empty;

    public async Task InitializeAsync()
    {
        var external = Environment.GetEnvironmentVariable("DFS_CLICKHOUSE_TEST_CONNECTION");
        if (!string.IsNullOrWhiteSpace(external))
        {
            _connectionString = external;
            return;
        }

        _container = new ClickHouseBuilder(ClickHouseImage)
            .WithResourceMapping(
                Encoding.UTF8.GetBytes(ClusterConfigXml),
                "/etc/clickhouse-server/config.d/nexus_analytics.xml")
            .Build();
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }

    [Fact]
    public void Apply_is_idempotent_and_lands_the_full_analytics_schema()
    {
        // An external target (DFS_CLICKHOUSE_TEST_CONNECTION) is assumed to be the replicated lab.
        var profile = _container is null
            ? ClickHouseMigrationProfile.Lab()
            : ClickHouseMigrationProfile.SingleNode();

        var first = ClickHouseMigrationRunner.MigrateUp(() => new ClickHouseConnection(_connectionString), profile);
        first.Successful.Should().BeTrue(first.Error);
        first.ScriptsExecuted.Should().NotBeEmpty("the first run applies every script");
        CountPresent().Should().Be(ClickHouseTables.All.Count, "every analytics object exists after the first apply");

        var second = ClickHouseMigrationRunner.MigrateUp(() => new ClickHouseConnection(_connectionString), profile);
        second.Successful.Should().BeTrue(second.Error);
        second.ScriptsExecuted.Should().BeEmpty("re-applying a fully-migrated schema is a no-op");
        CountPresent().Should().Be(ClickHouseTables.All.Count, "the schema is unchanged after the second apply");
    }

    private int CountPresent()
    {
        using var connection = new ClickHouseConnection(_connectionString);
        connection.Open();

        int present = 0;
        foreach (var name in ClickHouseTables.All)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT count() FROM system.tables WHERE database = 'analytics' AND name = '{name}'";
            present += Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        }

        return present;
    }
}
