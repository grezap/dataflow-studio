using System.Data.Common;
using System.Globalization;
using DataFlowStudio.Clickhouse;
using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.SharedKernel.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

// DataFlow Studio — "dfs telemetry": read back everything the pipeline recorded about itself, and
// prove both error paths still work (ADR-0008).
//
//   DataFlowStudio.Telemetry [verify | demo-errors | all]
//
//     verify       (default) query pipeline_events, cdc_lag_seconds, error_events, the latency MV,
//                  and the Kafka-engine ingestion objects.
//     demo-errors  emit one error through the NATIVE path (Kafka -> ClickHouse Kafka engine) and one
//                  through the .NET HTTPS control path, then wait for both to land.
//     all          demo-errors, then verify.
//
// Config (env): DFS_CLICKHOUSE_CONNECTION, DFS_CLICKHOUSE_CACERT (+ DFS_CLICKHOUSE_CLUSTER),
// and for demo-errors also DFS_KAFKA_BOOTSTRAP + DFS_KAFKA_CA/CERT/KEY. See dfs-telemetry.ps1.

var configuration = new ConfigurationBuilder().AddEnvironmentVariables().Build();

var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "verify";
if (mode is not ("verify" or "demo-errors" or "all"))
{
    await Console.Error.WriteLineAsync($"Unknown mode '{mode}'. Expected: verify | demo-errors | all.").ConfigureAwait(false);
    return 2;
}

var connectionString = configuration["DFS_CLICKHOUSE_CONNECTION"];
if (string.IsNullOrWhiteSpace(connectionString))
{
    await Console.Error.WriteLineAsync("Set DFS_CLICKHOUSE_CONNECTION (and DFS_CLICKHOUSE_CACERT for the lab's private CA).").ConfigureAwait(false);
    return 2;
}

var caCertPath = configuration["DFS_CLICKHOUSE_CACERT"];
var cluster = configuration["DFS_CLICKHOUSE_CLUSTER"] ?? "nexus_analytics";

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

using var loggerFactory = LoggerFactory.Create(b =>
    b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));

string? nativeTraceId = null;
string? httpsTraceId = null;

if (mode is "demo-errors" or "all")
{
    if (!TelemetryOptionsFactory.TryFromConfiguration(configuration, out var options))
    {
        await Console.Error.WriteLineAsync(
            "demo-errors needs Kafka too: set DFS_KAFKA_BOOTSTRAP and readable DFS_KAFKA_CA/CERT/KEY.").ConfigureAwait(false);
        return 2;
    }

    var stamp = DateTime.UtcNow.ToString("HHmmss", CultureInfo.InvariantCulture);
    nativeTraceId = $"native-proof-{stamp}";
    httpsTraceId = $"https-proof-{stamp}";

    var errorSink = new ClickHouseErrorSink(options, loggerFactory.CreateLogger<ClickHouseErrorSink>());
    Console.WriteLine("Emitting one error down each path…");

    // Path 1 — NATIVE: produced to dfs.telemetry.error_events; ClickHouse's Kafka-engine table and
    // its materialized view do the ingestion. No .NET consumer is involved.
    await using (var kafkaSink = new KafkaTelemetrySink(options, loggerFactory.CreateLogger<KafkaTelemetrySink>(), errorSink))
    {
        await kafkaSink.EnsureTopicsAsync(cts.Token).ConfigureAwait(false);
        kafkaSink.RecordError(new PipelineError(
            DateTimeOffset.UtcNow, nativeTraceId, "curation", "demo-native-path",
            "native Kafka-engine error ingestion proof", string.Empty));
        await kafkaSink.FlushAsync(cts.Token).ConfigureAwait(false);
    }

    Console.WriteLine($"  native  -> dfs.telemetry.error_events   (trace {nativeTraceId})");

    // Path 2 — HTTPS control path: the direct insert used when the broker is unreachable, i.e. when
    // the error is about Kafka itself and producing to it is not an option.
    await errorSink.InsertAsync(new PipelineError(
        DateTimeOffset.UtcNow, httpsTraceId, "telemetry", "demo-https-control-path",
        "direct ClickHouse HTTPS insert (the .NET control/fallback path)", "at Probe.Run()"),
        cts.Token).ConfigureAwait(false);

    Console.WriteLine($"  https   -> analytics.error_events        (trace {httpsTraceId})");

    // The native leg is asynchronous (produce -> broker -> ClickHouse poll -> MV), so wait for it.
    Console.WriteLine();
    Console.Write("Waiting for both to land in analytics.error_events");
    var landed = await WaitForBothAsync(nativeTraceId, httpsTraceId, TimeSpan.FromSeconds(90), cts.Token).ConfigureAwait(false);
    Console.WriteLine();
    Console.WriteLine(landed
        ? "  ✅ both paths landed — native ingestion and the HTTPS control path are both live."
        : "  ⚠️ timed out. The HTTPS row inserts synchronously, so if only the native one is missing see handbook §3.2 T20-T22 (ClickHouse Kafka client cert / ACL / <kafka> config).");
    Console.WriteLine();
}

if (mode is "verify" or "all")
{
    await using var connection = TlsClickHouseConnectionFactory.Create(connectionString, caCertPath);
    await connection.OpenAsync(cts.Token).ConfigureAwait(false);

    await ShowAsync(connection, "Kafka-engine ingestion objects (3 readers + 3 materialized views)",
        """
        SELECT name, engine FROM system.tables
        WHERE database = 'analytics' AND (name LIKE '%_kafka' OR name LIKE '%_kafka_mv')
        ORDER BY name
        """).ConfigureAwait(false);

    await ShowAsync(connection, "pipeline_events — per-stage latency, by pipeline",
        """
        SELECT pipeline, stage, count() AS rows, round(avg(duration_ms), 1) AS avg_ms
        FROM analytics.pipeline_events
        GROUP BY pipeline, stage ORDER BY pipeline, stage
        """).ConfigureAwait(false);

    await ShowAsync(connection, "cdc_lag_seconds — end-to-end freshness",
        """
        SELECT source, count() AS samples,
               round(min(lag_seconds), 1) AS min_lag,
               round(max(lag_seconds), 1) AS max_lag
        FROM analytics.cdc_lag_seconds GROUP BY source
        """).ConfigureAwait(false);

    // The materialized view has no Distributed wrapper, so it is read across shards with cluster().
    await ShowAsync(connection, "pipeline_latency_by_hour — p50/p95/p99 from aggregate states",
        $"""
        SELECT stage,
               countMerge(events_state) AS events,
               round(quantilesMerge(0.5, 0.95, 0.99)(p_state)[1], 1) AS p50,
               round(quantilesMerge(0.5, 0.95, 0.99)(p_state)[2], 1) AS p95,
               round(quantilesMerge(0.5, 0.95, 0.99)(p_state)[3], 1) AS p99
        FROM cluster('{cluster}', analytics.pipeline_latency_by_hour)
        GROUP BY stage ORDER BY events DESC
        """).ConfigureAwait(false);

    await ShowAsync(connection, "error_events — most recent",
        """
        SELECT event_time, trace_id, service, error_code, message
        FROM analytics.error_events ORDER BY event_time DESC LIMIT 10
        """).ConfigureAwait(false);
}

return 0;

// Prints a query as a simple aligned table.
async Task ShowAsync(DbConnection connection, string title, string sql)
{
    Console.WriteLine($"── {title} ".PadRight(100, '─'));
    using var command = connection.CreateCommand();
    command.CommandText = sql;

    await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
    var widths = new int[reader.FieldCount];
    var rows = new List<string[]>();
    for (int i = 0; i < reader.FieldCount; i++)
    {
        widths[i] = reader.GetName(i).Length;
    }

    while (await reader.ReadAsync().ConfigureAwait(false))
    {
        var row = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            row[i] = Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? string.Empty;
            widths[i] = Math.Max(widths[i], row[i].Length);
        }

        rows.Add(row);
    }

    var header = new string[reader.FieldCount];
    for (int i = 0; i < reader.FieldCount; i++)
    {
        header[i] = reader.GetName(i).PadRight(widths[i]);
    }

    Console.WriteLine("  " + string.Join("  ", header));
    foreach (var row in rows)
    {
        for (int i = 0; i < row.Length; i++)
        {
            row[i] = row[i].PadRight(widths[i]);
        }

        Console.WriteLine("  " + string.Join("  ", row));
    }

    if (rows.Count == 0)
    {
        Console.WriteLine("  (no rows — run dfs-curate.ps1 / dfs-warehouse-sink.ps1 first)");
    }

    Console.WriteLine();
}

// Polls until both demo errors are visible, so the asynchronous native leg is genuinely proven.
async Task<bool> WaitForBothAsync(string nativeId, string httpsId, TimeSpan timeout, CancellationToken ct)
{
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
    {
        try
        {
            await using var connection = TlsClickHouseConnectionFactory.Create(connectionString, caCertPath);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT uniqExact(trace_id) FROM analytics.error_events WHERE trace_id IN ('{nativeId}', '{httpsId}')";
            var found = Convert.ToInt32(await command.ExecuteScalarAsync(ct).ConfigureAwait(false), CultureInfo.InvariantCulture);
            if (found >= 2)
            {
                return true;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Transient while ClickHouse catches up; keep polling until the deadline.
        }

        Console.Write(".");
        await Task.Delay(TimeSpan.FromSeconds(3), ct).ConfigureAwait(false);
    }

    return false;
}
