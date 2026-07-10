using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.SharedKernel;

/// <summary>
/// A module in the DataFlow Studio modular monolith. Each module is an isolated assembly
/// that self-registers its services into the composition root (the Api host). Modules never
/// reference one another's internals — cross-module communication flows through SharedKernel
/// contracts (see <see cref="IntegrationEvent"/>), a boundary enforced by the architecture
/// tests (NetArchTest).
/// </summary>
public interface IModule
{
    /// <summary>Stable module name (used in logs, health checks, and OpenLineage job names).</summary>
    string Name { get; }

    /// <summary>Registers this module's services into the shared composition root.</summary>
    void RegisterServices(IServiceCollection services, IConfiguration configuration);
}
