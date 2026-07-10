using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Migrations.Oltp;

/// <summary>
/// Builds and drives the FluentMigrator runner for the OltpDb schema. Shared by the console
/// entrypoint (deploy-time / <c>nexus-cli deploy</c>) and the E1 up → down → up CI test, so a
/// single code path is exercised everywhere.
/// </summary>
public static class OltpMigrationRunner
{
    private static ServiceProvider BuildServiceProvider(string connectionString) =>
        new ServiceCollection()
            .AddFluentMigratorCore()
            .ConfigureRunner(rb => rb
                .AddSqlServer()
                .WithGlobalConnectionString(connectionString)
                .ScanIn(typeof(OltpMigrationRunner).Assembly).For.Migrations())
            .AddLogging(lb => lb.AddFluentMigratorConsole())
            .BuildServiceProvider(validateScopes: false);

    /// <summary>Applies every pending migration (schema forward to latest).</summary>
    public static void MigrateUp(string connectionString)
    {
        using var provider = BuildServiceProvider(connectionString);
        using var scope = provider.CreateScope();
        scope.ServiceProvider.GetRequiredService<IMigrationRunner>().MigrateUp();
    }

    /// <summary>Rolls every migration back to version 0 (drops the whole schema).</summary>
    public static void MigrateDownToZero(string connectionString)
    {
        using var provider = BuildServiceProvider(connectionString);
        using var scope = provider.CreateScope();
        scope.ServiceProvider.GetRequiredService<IMigrationRunner>().MigrateDown(0);
    }
}
