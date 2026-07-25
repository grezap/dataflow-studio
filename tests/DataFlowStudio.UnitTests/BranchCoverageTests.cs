using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DataFlowStudio.Lineage;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.UnitTests.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// Targeted branch coverage for the private-CA import paths (OTLP wiring + lineage factory) and the
/// lineage emitter's cancellation swallow — the paths a happy-path test doesn't reach.
/// </summary>
public sealed class BranchCoverageTests : IDisposable
{
    private readonly string _caPath;

    public BranchCoverageTests()
    {
        _caPath = Path.Combine(Path.GetTempPath(), "dfs-branch-ca-" + Guid.NewGuid().ToString("N") + ".pem");
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=dfs-branch-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        File.WriteAllText(_caPath, cert.ExportCertificatePem());
    }

    public void Dispose() => File.Delete(_caPath);

    private static IConfiguration Config(params (string Key, string? Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Fact]
    public void Observability_imports_the_private_ca_when_the_cacert_file_exists()
    {
        var ok = ObservabilityWiring.TryCreateOptions(
            Config(("DFS_OTLP_ENDPOINT", "https://otel:4318"), ("DFS_OTLP_CACERT", _caPath)), "dfs", out var options);

        ok.ShouldBeTrue();
        options.ServerCaCertificates.ShouldNotBeNull().Count.ShouldBe(1);
    }

    [Fact]
    public void Observability_leaves_the_ca_null_when_the_cacert_path_does_not_exist()
    {
        ObservabilityWiring.TryCreateOptions(
            Config(("DFS_OTLP_ENDPOINT", "https://otel:4318"), ("DFS_OTLP_CACERT", "/no/such/ca.pem")), "dfs", out var options);

        options.ServerCaCertificates.ShouldBeNull();
    }

    [Fact]
    public void Lineage_factory_ignores_a_cacert_path_that_does_not_exist()
    {
        var emitter = LineageEmitterFactory.Create(
            Config(("DFS_MARQUEZ_ENDPOINT", "https://marquez"), ("DFS_MARQUEZ_CACERT", "/no/such/ca.pem")),
            NullLoggerFactory.Instance);

        emitter.ShouldBeOfType<MarquezLineageEmitter>();
        (emitter as IDisposable)?.Dispose();
    }

    [Fact]
    public async Task Emitter_swallows_a_cancelled_send()
    {
        var handler = new StubHttpMessageHandler();
        using var emitter = new MarquezLineageEmitter(
            new LineageEmitterOptions { Endpoint = new Uri("https://marquez.test") },
            NullLogger<MarquezLineageEmitter>.Instance,
            handler);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // A cancelled token makes PostAsync throw TaskCanceledException → the emitter swallows it (best-effort).
        await Should.NotThrowAsync(() => emitter.StartAsync("curation", "run", ["in"], cts.Token));
    }
}
