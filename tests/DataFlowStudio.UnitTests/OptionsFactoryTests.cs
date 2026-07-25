using DataFlowStudio.Modules.Ingestion.Curation;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.Modules.Warehouse.Sink;
using Microsoft.Extensions.Configuration;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The host-config option factories (Curation / Warehouse-sink / Telemetry): each returns false when the
/// Kafka connection isn't wired (so a host boots without the live worker) and, when the mTLS PEM files
/// resolve, builds options carrying the connection + lab defaults. Secrets are referenced by file path.
/// </summary>
public sealed class OptionsFactoryTests : IDisposable
{
    private readonly string _dir;
    private readonly string _ca;
    private readonly string _cert;
    private readonly string _key;

    public OptionsFactoryTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "dfs-opt-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        _ca = WriteFile("ca.pem", "CA-PEM");
        _cert = WriteFile("cert.pem", "CERT-PEM");
        _key = WriteFile("key.pem", "KEY-PEM");
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // best-effort cleanup
        }
    }

    private string WriteFile(string name, string content)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, content);
        return path;
    }

    private static IConfiguration Config(params (string Key, string? Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    private (string Key, string? Value)[] Kafka() =>
    [
        ("DFS_KAFKA_BOOTSTRAP", "192.168.10.21:9092"),
        ("DFS_KAFKA_CA", _ca),
        ("DFS_KAFKA_CERT", _cert),
        ("DFS_KAFKA_KEY", _key),
    ];

    // ---- Curation ----

    [Fact]
    public void Curation_is_false_when_unconfigured()
    {
        CurationOptionsFactory.TryFromConfiguration(Config(), out _).ShouldBeFalse();
    }

    [Fact]
    public void Curation_is_false_when_a_pem_file_is_missing()
    {
        var config = Config(
            ("DFS_KAFKA_BOOTSTRAP", "192.168.10.21:9092"),
            ("DFS_KAFKA_CA", Path.Combine(_dir, "nope.pem")),
            ("DFS_KAFKA_CERT", _cert),
            ("DFS_KAFKA_KEY", _key));
        CurationOptionsFactory.TryFromConfiguration(config, out _).ShouldBeFalse();
    }

    [Fact]
    public void Curation_builds_options_reading_the_pem_material_and_defaults()
    {
        CurationOptionsFactory.TryFromConfiguration(Config(Kafka()), out var options).ShouldBeTrue();

        options.Kafka.BootstrapServers.ShouldBe("192.168.10.21:9092");
        options.Kafka.CaCertPem.ShouldBe("CA-PEM");
        options.SchemaRegistryUrl.ShouldBe("https://192.168.10.91:8081");
        options.ConsumerGroup.ShouldBe("dfs-curation");
    }

    [Fact]
    public void Curation_honours_overrides()
    {
        var config = Config([.. Kafka(), ("DFS_SR_URL", "https://sr:9000"), ("DFS_CURATION_GROUP", "grp")]);
        CurationOptionsFactory.TryFromConfiguration(config, out var options).ShouldBeTrue();

        options.SchemaRegistryUrl.ShouldBe("https://sr:9000");
        options.ConsumerGroup.ShouldBe("grp");
    }

    // ---- Warehouse sink ----

    [Fact]
    public void Warehouse_is_false_without_a_starrocks_connection()
    {
        WarehouseSinkOptionsFactory.TryFromConfiguration(Config(Kafka()), out _).ShouldBeFalse();
    }

    [Fact]
    public void Warehouse_builds_options_with_the_starrocks_connection()
    {
        var config = Config([.. Kafka(), ("DFS_STARROCKS_CONNECTION", "Server=sr;Port=9030"), ("DFS_WAREHOUSE_GROUP", "wh")]);
        WarehouseSinkOptionsFactory.TryFromConfiguration(config, out var options).ShouldBeTrue();

        options.StarRocksConnection.ShouldBe("Server=sr;Port=9030");
        options.ConsumerGroup.ShouldBe("wh");
    }

    // ---- Telemetry ----

    [Fact]
    public void Telemetry_is_false_when_unconfigured()
    {
        TelemetryOptionsFactory.TryFromConfiguration(Config(), out _).ShouldBeFalse();
    }

    [Fact]
    public void Telemetry_builds_kafka_only_options_with_optional_paths_null()
    {
        TelemetryOptionsFactory.TryFromConfiguration(Config(Kafka()), out var options).ShouldBeTrue();

        options.ClickHouseConnectionString.ShouldBeNull();
        options.OtlpEndpoint.ShouldBeNull();
        options.ServiceName.ShouldBe("dataflow-studio");
    }

    [Fact]
    public void Telemetry_wires_clickhouse_and_otlp_when_present()
    {
        var config = Config(
        [
            .. Kafka(),
            ("DFS_CLICKHOUSE_CONNECTION", "Host=ch;Database=analytics"),
            ("DFS_OTLP_ENDPOINT", "http://otel:4318"),
            ("DFS_OTEL_SERVICE", "dfs-curation"),
        ]);
        TelemetryOptionsFactory.TryFromConfiguration(config, out var options).ShouldBeTrue();

        options.ClickHouseConnectionString.ShouldBe("Host=ch;Database=analytics");
        options.OtlpEndpoint.ShouldBe(new Uri("http://otel:4318"));
        options.ServiceName.ShouldBe("dfs-curation");
    }
}
