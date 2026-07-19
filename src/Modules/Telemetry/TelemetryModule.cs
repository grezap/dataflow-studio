using DataFlowStudio.SharedKernel;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Nexus.Observability;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// The Telemetry module makes the pipeline observe itself. It owns the ClickHouse <c>analytics</c>
/// telemetry schema (migrated by <c>DataFlowStudio.Migrations.Clickhouse</c>) and supplies the
/// <see cref="IPipelineTelemetrySink"/> the pipeline stages emit through: per-stage latency and CDC-lag
/// flow natively to ClickHouse via Kafka (<c>dfs.telemetry.*</c> → Kafka-engine), and errors flow
/// natively too with a direct-HTTPS fallback (ADR-0008). It always registers a sink (a no-op when the
/// pipeline isn't wired) so the Ingestion / Warehouse engines can depend on the abstraction. E16: wires
/// OpenTelemetry export when an OTLP endpoint is configured (the obs tier is off otherwise).
/// </summary>
public sealed class TelemetryModule : IModule
{
    /// <inheritdoc />
    public string Name => "telemetry";

    /// <inheritdoc />
    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        if (TelemetryOptionsFactory.TryFromConfiguration(configuration, out var options))
        {
            services.AddSingleton(options);
            services.AddSingleton<ClickHouseErrorSink>();
            services.AddSingleton<KafkaTelemetrySink>();
            services.AddSingleton<IPipelineTelemetrySink>(sp => sp.GetRequiredService<KafkaTelemetrySink>());
            services.AddHostedService<TelemetryTopicInitializer>();

            // E16: export traces + metrics only when the collector endpoint is set; without it the
            // ActivitySource/Meter have no listeners and cost nothing (the obs tier lands in 3E).
            if (options.OtlpEndpoint is not null)
            {
                services.AddNexusObservability(new ObservabilityOptions
                {
                    ServiceName = options.ServiceName,
                    OtlpEndpoint = options.OtlpEndpoint,
                });
            }
        }
        else
        {
            // Always provide a sink so the pipeline engines resolve their telemetry dependency.
            services.AddSingleton<IPipelineTelemetrySink>(NullPipelineTelemetrySink.Instance);
        }
    }
}
