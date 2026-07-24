using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Configuration;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The OTLP export wiring (E16, ADR-0010) must be off unless an endpoint is configured, and when on it
/// must register the pipeline ActivitySource + the telemetry Meter so both traces and metrics export.
/// </summary>
public sealed class ObservabilityWiringTests
{
    private static IConfiguration Config(params (string Key, string Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Fact]
    public void TryCreateOptions_is_false_when_no_endpoint_is_set()
    {
        ObservabilityWiring.TryCreateOptions(Config(), "dfs-curation", out _).ShouldBeFalse();
    }

    [Fact]
    public void TryCreateOptions_registers_the_pipeline_source_and_meter()
    {
        var ok = ObservabilityWiring.TryCreateOptions(
            Config(("DFS_OTLP_ENDPOINT", "https://192.168.70.182:4318")), "dfs-curation", out var options);

        ok.ShouldBeTrue();
        options.ServiceName.ShouldBe("dfs-curation");
        options.OtlpEndpoint.ShouldBe(new Uri("https://192.168.70.182:4318"));
        options.AdditionalSources.ShouldNotBeNull().ShouldContain(DataflowActivity.SourceName);
        options.AdditionalMeters.ShouldNotBeNull().ShouldContain(KafkaTelemetrySink.MeterName);
    }

    [Fact]
    public void TryCreateOptions_leaves_the_ca_null_when_unset()
    {
        ObservabilityWiring.TryCreateOptions(
            Config(("DFS_OTLP_ENDPOINT", "https://192.168.70.182:4318")), "dfs-curation", out var options);

        options.ServerCaCertificates.ShouldBeNull();
    }
}
