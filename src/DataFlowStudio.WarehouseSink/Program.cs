using DataFlowStudio.Modules.Warehouse.Sink;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

// DataFlow Studio — "dfs warehouse-sink": load the StarRocks Kimball DWH from the curated Avro topics
// in one drain pass — consume the current snapshot, SCD2-load the dimensions, reload the facts, then
// print a per-entity count. The hosted WarehouseSinkWorker runs the same engine on a timer.
//
// Config (env): DFS_KAFKA_BOOTSTRAP, DFS_KAFKA_CA/CERT/KEY (PEM paths), DFS_SR_URL,
// DFS_STARROCKS_CONNECTION. See dfs-warehouse-sink.ps1.

var configuration = new ConfigurationBuilder().AddEnvironmentVariables().Build();

if (!WarehouseSinkOptionsFactory.TryFromConfiguration(configuration, out var options))
{
    await Console.Error.WriteLineAsync(
        "Sink not configured. Set DFS_KAFKA_BOOTSTRAP, DFS_KAFKA_CA/CERT/KEY, DFS_STARROCKS_CONNECTION (+ optional DFS_SR_URL).")
        .ConfigureAwait(false);
    return 2;
}

// A fresh consumer group each run so drain replays every curated record from the beginning. The
// base group comes from DFS_WAREHOUSE_GROUP (default dfs-warehouse-sink) so it can be pointed at an
// authorized ACL prefix in the lab.
options = options with { ConsumerGroup = options.ConsumerGroup + "-" + Guid.NewGuid().ToString("N")[..8] };

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Information));

var engine = new WarehouseSinkEngine(options, loggerFactory.CreateLogger<WarehouseSinkEngine>());

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var counts = await engine.RunAsync(cts.Token).ConfigureAwait(false);

Console.WriteLine();
Console.WriteLine("Curated records consumed + loaded per entity:");
foreach (var (entity, count) in counts.OrderByDescending(kv => kv.Value))
{
    Console.WriteLine($"  {entity,-22} {count}");
}

Console.WriteLine($"  {"TOTAL",-22} {counts.Values.Sum()}");
return 0;
