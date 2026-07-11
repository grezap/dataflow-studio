using DataFlowStudio.SharedKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Modules.Warehouse;

/// <summary>
/// The Warehouse module owns the StarRocks Kimball DWH (<c>dwh</c> star schema + <c>analytics</c>
/// serving). It consumes the Kafka Avro stream and loads dimensions (SCD2) and facts.
/// Migrations are DbUp SQL scripts (E1). Populated in the Week-3 DWH slice.
/// </summary>
public sealed class WarehouseModule : IModule
{
    /// <inheritdoc />
    public string Name => "warehouse";

    /// <inheritdoc />
    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        // Week 3: StarRocks loaders + DbUp migration runner.
    }
}
