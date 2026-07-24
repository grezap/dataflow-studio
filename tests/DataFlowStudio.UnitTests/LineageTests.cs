using DataFlowStudio.Lineage;
using DataFlowStudio.SharedKernel.Lineage;
using Microsoft.Extensions.Configuration;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The OpenLineage emitter (E16, ADR-0011) must be off unless a Marquez endpoint is configured, and when
/// on it must carry the endpoint + namespace. The no-op emitter must be inert so an unwired run is clean.
/// </summary>
public sealed class LineageTests
{
    private static IConfiguration Config(params (string Key, string Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Fact]
    public void TryFromConfiguration_is_false_when_no_endpoint_is_set()
    {
        LineageEmitterFactory.TryFromConfiguration(Config(), out _).ShouldBeFalse();
    }

    [Fact]
    public void TryFromConfiguration_carries_the_endpoint_and_default_namespace()
    {
        var ok = LineageEmitterFactory.TryFromConfiguration(
            Config(("DFS_MARQUEZ_ENDPOINT", "https://192.168.70.127")), out var options);

        ok.ShouldBeTrue();
        options.Endpoint.ShouldBe(new Uri("https://192.168.70.127"));
        options.Namespace.ShouldBe("dataflow-studio");
    }

    [Fact]
    public void TryFromConfiguration_honours_an_explicit_namespace()
    {
        LineageEmitterFactory.TryFromConfiguration(
            Config(("DFS_MARQUEZ_ENDPOINT", "https://192.168.70.127"), ("DFS_MARQUEZ_NAMESPACE", "custom-ns")),
            out var options);

        options.Namespace.ShouldBe("custom-ns");
    }

    [Fact]
    public async Task NullLineageEmitter_is_inert()
    {
        var emitter = NullLineageEmitter.Instance;
        await Should.NotThrowAsync(async () =>
        {
            await emitter.StartAsync("job", "run", ["in"]);
            await emitter.CompleteAsync("job", "run", ["in"], ["out"]);
            await emitter.FailAsync("job", "run", ["in"], "boom");
        });
    }
}
