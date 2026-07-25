using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

// DataFlow Studio — .NET Aspire AppHost. Composes the modular-monolith Api (always-on) with the runnable
// pipeline consoles as first-class resources, so `dotnet run` on this project brings up the Aspire
// dashboard showing the whole topology — the Api's endpoints + logs, and each worker as a node you can
// start on demand. Module isolation and the no-EF-on-AOT invariant (ADR-0007) are untouched: the AppHost
// only orchestrates the existing projects; it references no module code.

var builder = DistributedApplication.CreateBuilder(args);

// The pipeline's runtime wiring (Kafka mTLS PEM paths, sink connections, the OTLP + Marquez endpoints,
// the OLTP connection) is supplied by the environment / Vault at deploy time. The AppHost forwards
// whatever is set so every composed resource sees the same configuration the standalone consoles use.
string[] pipelineEnv =
[
    "DFS_SQL_CONN",
    "DFS_KAFKA_BOOTSTRAP", "DFS_KAFKA_CA", "DFS_KAFKA_CERT", "DFS_KAFKA_KEY",
    "DFS_SR_URL", "DFS_STARROCKS_CONNECTION",
    "DFS_CLICKHOUSE_CONNECTION", "DFS_CLICKHOUSE_CACERT",
    "DFS_OTLP_ENDPOINT", "DFS_OTLP_CACERT",
    "DFS_MARQUEZ_ENDPOINT", "DFS_MARQUEZ_CACERT",
];

// The Api composition root — always on. Exposes /health + /modules and, when the pipeline env is wired,
// hosts the continuous curation + warehouse-sink workers + the telemetry topic initializer.
Forward(builder.AddProject<Projects.DataFlowStudio_Api>("dfs-api"));

// The runnable pipeline consoles, modeled as first-class resources so the whole topology is visible.
// They are drain / demo jobs — each runs once against the lab tiers, then exits — so they are declared
// explicit-start: launch them from the dashboard, in order (seed → curate → warehouse-sink), rather than
// auto-running (and immediately exiting "not configured") at boot.
Forward(builder.AddProject<Projects.DataFlowStudio_Seed>("dfs-seed").WithExplicitStart());
Forward(builder.AddProject<Projects.DataFlowStudio_Curation>("dfs-curation").WithExplicitStart());
Forward(builder.AddProject<Projects.DataFlowStudio_WarehouseSink>("dfs-warehouse-sink").WithExplicitStart());
Forward(builder.AddProject<Projects.DataFlowStudio_Trace>("dfs-trace").WithExplicitStart());
Forward(builder.AddProject<Projects.DataFlowStudio_Telemetry>("dfs-telemetry-verify").WithExplicitStart());

builder.Build().Run();

// Forwards each set pipeline env var from the AppHost's own environment/config to a composed resource.
IResourceBuilder<ProjectResource> Forward(IResourceBuilder<ProjectResource> resource)
{
    foreach (var key in pipelineEnv)
    {
        var value = builder.Configuration[key];
        if (!string.IsNullOrWhiteSpace(value))
        {
            resource.WithEnvironment(key, value);
        }
    }

    return resource;
}
