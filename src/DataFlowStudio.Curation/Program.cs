using DataFlowStudio.Modules.Ingestion.Curation;
using DataFlowStudio.Modules.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

// DataFlow Studio — "dfs curate": run the CDC curation engine in DRAIN mode. It consumes every raw
// Debezium topic in the catalog, reshapes each change into curated Avro, produces it to the
// dfs.<entity>.changed.v1 topics, and stops once the raw snapshot is fully consumed — then prints a
// per-entity count. The hosted CurationWorker (Api) runs the same engine continuously.
//
// Config (env): DFS_KAFKA_BOOTSTRAP, DFS_KAFKA_CA/CERT/KEY (PEM paths), DFS_SR_URL. See dfs-curate.ps1.

var configuration = new ConfigurationBuilder().AddEnvironmentVariables().Build();

if (!CurationOptionsFactory.TryFromConfiguration(configuration, out var options))
{
    await Console.Error.WriteLineAsync(
        "Kafka connection not configured. Set DFS_KAFKA_BOOTSTRAP and readable DFS_KAFKA_CA/CERT/KEY (+ optional DFS_SR_URL).")
        .ConfigureAwait(false);
    return 2;
}

// A fresh consumer group each run so drain replays every raw record from the beginning.
options = options with { ConsumerGroup = "dfs-curation-drain-" + Guid.NewGuid().ToString("N")[..8] };

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Information));

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// Telemetry (ADR-0008): a live Kafka sink when DFS_KAFKA_* is set (same env as curation), else a no-op.
// The curation engine emits per-record stage latency + CDC lag through it as it drains.
var telemetry = await TelemetrySinkFactory.CreateAsync(configuration, loggerFactory, cts.Token).ConfigureAwait(false);

// E16: start OTLP export (curate spans + the emit counter) when DFS_OTLP_ENDPOINT is set. Disposed after
// the drain so the final spans + metric collection flush to the lab collector (traces→Tempo, metrics→Prom).
var serviceName = configuration["DFS_OTEL_SERVICE"] ?? "dataflow-studio";
using var observability = ObservabilityConsole.TryStart(configuration, serviceName);

var engine = new CurationEngine(options, loggerFactory.CreateLogger<CurationEngine>(), telemetry);

var counts = await engine.RunAsync(drainMode: true, cts.Token).ConfigureAwait(false);

if (telemetry is IAsyncDisposable telemetryDisposable)
{
    await telemetryDisposable.DisposeAsync().ConfigureAwait(false);
}

Console.WriteLine();
Console.WriteLine("Curated records per entity:");
foreach (var (entity, count) in counts.OrderByDescending(kv => kv.Value))
{
    Console.WriteLine($"  {entity,-22} {count}");
}

Console.WriteLine($"  {"TOTAL",-22} {counts.Values.Sum()}");
return 0;
