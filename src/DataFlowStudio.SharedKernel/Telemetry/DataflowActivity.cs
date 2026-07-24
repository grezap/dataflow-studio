using System.Diagnostics;

namespace DataFlowStudio.SharedKernel.Telemetry;

/// <summary>
/// The pipeline's distributed-tracing seam. It exposes the single <see cref="ActivitySource"/> the
/// pipeline stages (curation, warehouse-sink) start spans from — defined in the SharedKernel, next to
/// <see cref="IPipelineTelemetrySink"/>, so a module never references the Telemetry module (module
/// isolation, ADR-0001). The OTel exporter is wired to <see cref="SourceName"/> by the host/console
/// (E16); until then <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> returns
/// <see langword="null"/> — there are no listeners, so instrumentation is free.
/// <para>
/// <see cref="SourceName"/> deliberately equals the OTel service name (<c>dataflow-studio</c>) so
/// <c>AddNexusObservability</c>'s default <c>AddSource(serviceName)</c> already captures it; hosts may
/// also register it explicitly via the additional-sources list to decouple the two.
/// </para>
/// </summary>
public static class DataflowActivity
{
    /// <summary>The ActivitySource name — equal to the OTel service name so exporters capture it by default.</summary>
    public const string SourceName = "dataflow-studio";

    /// <summary>The shared ActivitySource every pipeline stage starts its spans from.</summary>
    public static readonly ActivitySource Source = new(SourceName);
}
