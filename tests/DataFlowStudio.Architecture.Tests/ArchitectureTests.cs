using System.Reflection;
using DataFlowStudio.Migrations.Oltp;
using DataFlowStudio.Modules.Commerce;
using DataFlowStudio.Modules.Ingestion;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.Modules.Warehouse;
using DataFlowStudio.SharedKernel;
using FluentAssertions;
using NetArchTest.Rules;
using Xunit;

namespace DataFlowStudio.Architecture.Tests;

/// <summary>
/// Enforces the modular-monolith boundaries (MASTER-PLAN skill dimension 1 + ADR-0001) and the
/// E4 "no EF Core on AOT paths" rule (ADR-0002). These tests fail the build if the architecture
/// erodes.
/// </summary>
public sealed class ArchitectureTests
{
    private static readonly Assembly SharedKernel = typeof(IModule).Assembly;
    private static readonly Assembly Commerce = typeof(CommerceModule).Assembly;
    private static readonly Assembly Ingestion = typeof(IngestionModule).Assembly;
    private static readonly Assembly Warehouse = typeof(WarehouseModule).Assembly;
    private static readonly Assembly Telemetry = typeof(TelemetryModule).Assembly;
    private static readonly Assembly MigrationsOltp = typeof(OltpMigrationRunner).Assembly;

    private const string CommerceNs = "DataFlowStudio.Modules.Commerce";
    private const string IngestionNs = "DataFlowStudio.Modules.Ingestion";
    private const string WarehouseNs = "DataFlowStudio.Modules.Warehouse";
    private const string TelemetryNs = "DataFlowStudio.Modules.Telemetry";
    private const string ApiNs = "DataFlowStudio.Api";
    private const string EntityFrameworkCoreNs = "Microsoft.EntityFrameworkCore";

    [Fact]
    public void Commerce_should_not_depend_on_other_modules_or_the_host()
        => AssertNoDependency(Commerce, IngestionNs, WarehouseNs, TelemetryNs, ApiNs);

    [Fact]
    public void Ingestion_should_not_depend_on_other_modules_or_the_host()
        => AssertNoDependency(Ingestion, CommerceNs, WarehouseNs, TelemetryNs, ApiNs);

    [Fact]
    public void Warehouse_should_not_depend_on_other_modules_or_the_host()
        => AssertNoDependency(Warehouse, CommerceNs, IngestionNs, TelemetryNs, ApiNs);

    [Fact]
    public void Telemetry_should_not_depend_on_other_modules_or_the_host()
        => AssertNoDependency(Telemetry, CommerceNs, IngestionNs, WarehouseNs, ApiNs);

    [Fact]
    public void SharedKernel_should_not_depend_on_any_module_or_the_host()
        => AssertNoDependency(SharedKernel, CommerceNs, IngestionNs, WarehouseNs, TelemetryNs, ApiNs);

    [Fact]
    public void Aot_paths_should_not_depend_on_ef_core()
    {
        // E4 (ADR-0002): the Ingestion Kafka worker is Native-AOT and the migration tool uses
        // FluentMigrator + raw SQL — neither may pull in EF Core.
        AssertNoDependency(Ingestion, EntityFrameworkCoreNs);
        AssertNoDependency(MigrationsOltp, EntityFrameworkCoreNs);
    }

    private static void AssertNoDependency(Assembly assembly, params string[] forbiddenNamespaces)
    {
        var result = Types.InAssembly(assembly)
            .ShouldNot()
            .HaveDependencyOnAny(forbiddenNamespaces)
            .GetResult();

        result.IsSuccessful.Should().BeTrue(
            $"{assembly.GetName().Name} must not depend on [{string.Join(", ", forbiddenNamespaces)}]; " +
            $"offending types: {string.Join(", ", result.FailingTypeNames ?? [])}");
    }
}
