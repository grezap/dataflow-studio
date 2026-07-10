using DataFlowStudio.SharedKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataFlowStudio.Modules.Telemetry;

/// <summary>
/// The Telemetry module owns the ClickHouse <c>analytics</c> schema — pipeline events, CDC lag,
/// and error events (Replicated/Distributed + AggregatingMergeTree MV). It records the pipeline's
/// own operational telemetry. Migrations are DbUp SQL scripts (E1). Populated in the Week-3 slice.
/// </summary>
public sealed class TelemetryModule : IModule
{
    public string Name => "telemetry";

    public void RegisterServices(IServiceCollection services, IConfiguration configuration)
    {
        // Week 3: ClickHouse writers (pipeline_events / cdc_lag_seconds / error_events).
    }
}
