using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DataFlowStudio.Lineage;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.Modules.Warehouse.Sink;
using DataFlowStudio.SharedKernel;
using DataFlowStudio.SharedKernel.Lineage;
using DataFlowStudio.UnitTests.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>Focused tests for the small shared building blocks: the integration-event base, the curated
/// Avro field readers, the telemetry topic set, and the lineage-emitter factory (including the private-CA
/// path).</summary>
public sealed class MiscTests
{
    private sealed record TestEvent : IntegrationEvent
    {
        public override string Subject => "oltp.test";
    }

    [Fact]
    public void IntegrationEvent_defaults_id_and_timestamp()
    {
        var e = new TestEvent();
        e.EventId.ShouldNotBe(Guid.Empty);
        e.OccurredUtc.ShouldBeGreaterThan(DateTimeOffset.UnixEpoch);
        e.Subject.ShouldBe("oltp.test");
    }

    [Fact]
    public void Rec_reads_typed_fields_and_raw()
    {
        var record = AvroRecord.Of(
            "r",
            ("id", "long", 42L),
            ("count", "int", 7),
            ("name", "string", "Ada"),
            ("maybe", "string", null));

        Rec.Long(record, "id").ShouldBe(42L);
        Rec.Int(record, "count").ShouldBe(7);
        Rec.Str(record, "name").ShouldBe("Ada");
        Rec.Str(record, "maybe").ShouldBe(string.Empty);   // null → empty
        Rec.Raw(record, "id").ShouldBe(42L);
    }

    [Fact]
    public void TelemetryTopics_all_lists_the_three_prefixed_topics()
    {
        TelemetryTopics.All.ShouldBe(
        [
            "dfs.telemetry.pipeline_events",
            "dfs.telemetry.cdc_lag",
            "dfs.telemetry.error_events",
        ]);
    }

    private static IConfiguration Config(params (string Key, string? Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Fact]
    public void LineageFactory_returns_the_noop_emitter_when_unconfigured()
    {
        var emitter = LineageEmitterFactory.Create(Config(), NullLoggerFactory.Instance);
        emitter.ShouldBeSameAs(NullLineageEmitter.Instance);
    }

    [Fact]
    public void LineageFactory_creates_a_live_emitter_when_an_endpoint_is_set()
    {
        var emitter = LineageEmitterFactory.Create(
            Config(("DFS_MARQUEZ_ENDPOINT", "https://192.168.70.127")), NullLoggerFactory.Instance);
        emitter.ShouldBeOfType<MarquezLineageEmitter>();
        (emitter as IDisposable)?.Dispose();
    }

    [Fact]
    public void LineageFactory_imports_a_private_ca_and_builds_the_pinning_emitter()
    {
        // A real PEM so ImportFromPemFile + the private-CA pinning handler are exercised.
        var caPath = Path.Combine(Path.GetTempPath(), "dfs-ca-" + Guid.NewGuid().ToString("N") + ".pem");
        using (var rsa = RSA.Create(2048))
        {
            var req = new CertificateRequest("CN=dfs-test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
            File.WriteAllText(caPath, cert.ExportCertificatePem());
        }

        try
        {
            var emitter = LineageEmitterFactory.Create(
                Config(("DFS_MARQUEZ_ENDPOINT", "https://192.168.70.127"), ("DFS_MARQUEZ_CACERT", caPath)),
                NullLoggerFactory.Instance);
            emitter.ShouldBeOfType<MarquezLineageEmitter>();
            (emitter as IDisposable)?.Dispose();
        }
        finally
        {
            File.Delete(caPath);
        }
    }
}
