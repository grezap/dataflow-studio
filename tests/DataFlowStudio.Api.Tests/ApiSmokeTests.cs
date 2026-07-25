using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using Xunit;

namespace DataFlowStudio.Api.Tests;

/// <summary>
/// Boots the Api composition root in-process (<see cref="WebApplicationFactory{TEntryPoint}"/>) and
/// verifies it comes up cleanly with the four modular-monolith modules wired — no live Kafka / StarRocks
/// / ClickHouse configured, so each module registers its no-op path. This guards the DI wiring end-to-end
/// (e.g. the telemetry sink + its error-fallback seam resolving).
/// </summary>
public sealed class ApiSmokeTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;

    public ApiSmokeTests(WebApplicationFactory<Program> factory) => _factory = factory;

    private sealed record Health(string Status, int ModuleCount);

    [Fact]
    public async Task Health_endpoint_reports_healthy_with_all_modules_wired()
    {
        using var client = _factory.CreateClient();

        using var response = await client.GetAsync(new Uri("/health", UriKind.Relative));

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        var health = await response.Content.ReadFromJsonAsync<Health>();
        health.ShouldNotBeNull();
        health.Status.ShouldBe("healthy");
        health.ModuleCount.ShouldBe(4);
    }

    [Fact]
    public async Task Modules_endpoint_lists_the_four_modules()
    {
        using var client = _factory.CreateClient();

        var modules = await client.GetFromJsonAsync<string[]>(new Uri("/modules", UriKind.Relative));

        modules.ShouldNotBeNull();
        modules.ShouldBe(["commerce", "ingestion", "warehouse", "telemetry"]);
    }
}
