using DataFlowStudio.Modules.Commerce;
using DataFlowStudio.Modules.Ingestion;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.Modules.Warehouse;
using DataFlowStudio.SharedKernel;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddProblemDetails();
builder.Services.AddOpenApi();

// The modular-monolith composition root. Each module is instantiated explicitly (no reflection —
// keeps the host trim/AOT-friendly) and self-registers its services. This is the only place the
// modules are wired together; they never reference one another (enforced by the architecture tests).
IReadOnlyList<IModule> modules =
[
    new CommerceModule(),
    new IngestionModule(),
    new WarehouseModule(),
    new TelemetryModule(),
];

foreach (var module in modules)
{
    module.RegisterServices(builder.Services, builder.Configuration);
}

var app = builder.Build();

app.UseStatusCodePages();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapGet("/health", () => Results.Ok(new HealthResponse("healthy", modules.Count)))
   .WithName("Health")
   .WithSummary("Liveness probe for the DataFlow Studio host.");

app.MapGet("/modules", () => Results.Ok(modules.Select(m => m.Name).ToArray()))
   .WithName("Modules")
   .WithSummary("Lists the registered modular-monolith modules.");

app.Run();

/// <summary>Payload of the <c>/health</c> endpoint: liveness status and the number of loaded modules.</summary>
/// <param name="Status">Health string (currently always <c>healthy</c> when the host is up).</param>
/// <param name="ModuleCount">How many modules the composition root wired in.</param>
internal sealed record HealthResponse(string Status, int ModuleCount);

/// <summary>
/// The application entry point. Declared <c>public partial</c> (rather than the implicit internal
/// top-level program) so a future <c>WebApplicationFactory&lt;Program&gt;</c> integration test can
/// boot the host in-process.
/// </summary>
public partial class Program;
