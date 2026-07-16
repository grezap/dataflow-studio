using DataFlowStudio.Modules.Warehouse.Sink;
using DataFlowStudio.SharedKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Modules.Warehouse;

/// <summary>
/// The Warehouse module owns the StarRocks Kimball DWH (<c>dwh</c> star + <c>analytics</c> serving).
/// It consumes the curated Avro topics and loads SCD2 dimensions + facts (ADR-0006). Its schema is
/// migrated by <c>DataFlowStudio.Migrations.Starrocks</c> (DbUp, ADR-0005). Non-AOT (Avro), no EF Core.
/// </summary>
public sealed class WarehouseModule : IModule
{
    /// <inheritdoc />
    public string Name => "warehouse";

    /// <inheritdoc />
    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        // Wire the live DWH sink only when Kafka + StarRocks are configured (env / Vault).
        if (WarehouseSinkOptionsFactory.TryFromConfiguration(configuration, out var options))
        {
            services.AddSingleton(options);
            services.AddSingleton<WarehouseSinkEngine>();
            services.AddHostedService<WarehouseSinkWorker>();
        }
    }
}
