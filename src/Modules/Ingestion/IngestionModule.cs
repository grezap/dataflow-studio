using DataFlowStudio.Modules.Ingestion.Curation;
using DataFlowStudio.SharedKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Modules.Ingestion;

/// <summary>
/// The Ingestion module is the CDC curation worker. It consumes Debezium's raw CDC topics and
/// re-produces clean, schema-registered Avro on the curated topics, driven by the data-driven
/// <see cref="Curation.CurationCatalog"/> (one spec per order-flow entity). It never references
/// another module — the raw/curated contract is Kafka + Avro (enforced by the architecture tests).
/// Non-AOT (ADR-0003/0007) but no EF Core.
/// </summary>
public sealed class IngestionModule : IModule
{
    /// <inheritdoc />
    public string Name => "ingestion";

    /// <inheritdoc />
    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        // Only wire the live curation worker when the Kafka connection is configured (env / Vault),
        // so the host still boots in environments where the pipeline isn't wired up.
        if (CurationOptionsFactory.TryFromConfiguration(configuration, out var options))
        {
            services.AddSingleton(options);
            services.AddSingleton<CurationEngine>();
            services.AddHostedService<CurationWorker>();
        }
    }
}
