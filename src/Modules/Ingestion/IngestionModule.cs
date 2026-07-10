using DataFlowStudio.SharedKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Modules.Ingestion;

/// <summary>
/// The Ingestion module is the CDC → Kafka pump. It reads change-data-capture rows from
/// OltpDb (via Dapper) and publishes them as Avro records through the Schema Registry. It is
/// deliberately generic: it operates on the SharedKernel <see cref="IntegrationEvent"/>
/// abstraction and a mapper registry populated by the composition root, so it never
/// references another module (enforced by the architecture tests). This is the E4 Native-AOT
/// worker path — Dapper only, no EF Core.
/// </summary>
public sealed class IngestionModule : IModule
{
    public string Name => "ingestion";

    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddHostedService<CdcPublishWorker>();
    }
}
