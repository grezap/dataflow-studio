using DataFlowStudio.Modules.Ingestion.Curation;
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

var engine = new CurationEngine(options, loggerFactory.CreateLogger<CurationEngine>());

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var counts = await engine.RunAsync(drainMode: true, cts.Token).ConfigureAwait(false);

Console.WriteLine();
Console.WriteLine("Curated records per entity:");
foreach (var (entity, count) in counts.OrderByDescending(kv => kv.Value))
{
    Console.WriteLine($"  {entity,-22} {count}");
}

Console.WriteLine($"  {"TOTAL",-22} {counts.Values.Sum()}");
return 0;
