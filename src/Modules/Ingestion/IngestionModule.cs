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
    /// <inheritdoc />
    public string Name => "ingestion";

    /// <inheritdoc />
    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        // Register the long-running CDC pump as a hosted background service so the Api host owns
        // its lifetime (started on boot, gracefully stopped on shutdown).
        services.AddHostedService<CdcPublishWorker>();
    }
}
