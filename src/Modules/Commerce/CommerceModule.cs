using DataFlowStudio.SharedKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Modules.Commerce;

/// <summary>
/// The Commerce module owns the OltpDb source-of-truth schema (11 tables, temporal +
/// audit columns) and the write-side domain. Its migrations live in
/// <c>DataFlowStudio.Migrations.Oltp</c>; its CDC changes flow out via
/// <see cref="Contracts.CustomerChangedEvent"/> and siblings toward the Ingestion worker.
/// </summary>
public sealed class CommerceModule : IModule
{
    /// <inheritdoc />
    public string Name => "commerce";

    /// <inheritdoc />
    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        // Week-1 slice: schema + migrations. Repositories / command handlers land with the
        // Commerce write-side slice. Registration is intentionally a no-op for now so the
        // composition root can already discover and wire every module uniformly.
    }
}
