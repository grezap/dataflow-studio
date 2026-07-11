using System.Globalization;
using System.Text.Json;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Microsoft.Data.SqlClient;
using Nexus.Avro;
using Nexus.Kafka;

namespace DataFlowStudio.Trace;

// DataFlow Studio — "dfs trace": follow ONE record across all five faces of the pipeline, printing
// what it looks like at each hop so you can see the data travel analytically.
//
//   Face 1  OLTP write        SQL Server OltpDb (the source of truth)
//   Face 2  CDC capture       SQL Server change table (cdc.dbo_Customers_CT)
//   Face 3  Debezium raw      Kafka topic oltp.OltpDb.dbo.Customers (JSON CDC envelope)
//   Face 4  Curated Avro      Kafka topic dfs.customers.changed.v1 (schema-registered Avro)
//   Face 5  Sink              an in-memory projection (StarRocks/ClickHouse land in Week 3)
//
// Config comes from environment variables (see Config.FromEnvironment). Secrets never live in code.
internal static class Program
{
    private const string RawTopic = "oltp.OltpDb.dbo.Customers";
    private const string CuratedTopic = "dfs.customers.changed.v1";

    // The curated domain event schema — self-describing, registered in the Schema Registry on first
    // publish. This is the clean shape downstream consumers (the DWH + telemetry) read, decoupled
    // from Debezium's raw envelope.
    private const string CuratedAvroSchema = """
        {
          "type": "record",
          "name": "CustomerChanged",
          "namespace": "com.nexusplatform.dataflowstudio.curated",
          "fields": [
            {"name": "customerId", "type": "long"},
            {"name": "customerCode", "type": "string"},
            {"name": "displayName", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "status", "type": "int"},
            {"name": "lifetimeValueUsd", "type": "string"},
            {"name": "operation", "type": "string"},
            {"name": "sourceTsMs", "type": "long"},
            {"name": "curatedAtUtc", "type": "long"}
          ]
        }
        """;

    private static async Task<int> Main()
    {
        var cfg = Config.FromEnvironment();
        // A unique natural key per run so we can unambiguously follow THIS record through every hop.
        var code = "TRACE-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
        var name = "Trace Customer " + code;

        Banner("DataFlow Studio — dfs trace", $"following customer '{code}' across 5 faces");

        try
        {
            long customerId = await Face1_OltpWriteAsync(cfg, code, name).ConfigureAwait(false);
            await Face2_CdcCaptureAsync(cfg, code).ConfigureAwait(false);
            var raw = await Face3_DebeziumRawAsync(cfg, code).ConfigureAwait(false);
            await Face4_CuratedAvroAsync(cfg, raw).ConfigureAwait(false);
            Face5_Sink(raw);

            Banner("DONE", $"customer {customerId} ('{code}') traversed OLTP → CDC → Debezium → curated Avro → sink.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"\nTRACE FAILED: {ex.Message}");
            return 1;
        }
    }

    // ── Face 1 ─ write a customer into OltpDb (the source of truth) ────────────────────────────
    private static async Task<long> Face1_OltpWriteAsync(Config cfg, string code, string name)
    {
        Face(1, "OLTP write", "insert a customer into SQL Server OltpDb (the source of truth)");
        await using var conn = new SqlConnection(cfg.SqlConnection);
        await conn.OpenAsync().ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO dbo.Customers (CustomerCode, DisplayName, Email, PreferredLocale, Status, LifetimeValueUsd, created_by, modified_by)
            OUTPUT INSERTED.CustomerId, CONVERT(bigint, INSERTED.row_version) AS row_version, INSERTED.created_utc
            VALUES (@code, @name, @email, 'en-US', 1, 42.50, 'dfs-trace', 'dfs-trace');
            """;
        cmd.Parameters.AddWithValue("@code", code);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@email", code.ToLowerInvariant() + "@example.com");
        await using var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        await r.ReadAsync().ConfigureAwait(false);
        long id = r.GetInt64(0);
        Console.WriteLine($"  inserted CustomerId={id}  Code={code}  ROWVERSION={r.GetInt64(1)}  created_utc={r.GetDateTime(2):o}");
        Console.WriteLine("  → the row now exists in the transaction log; the pipeline reads it AFTER the fact (zero contention).");
        return id;
    }

    // ── Face 2 ─ SQL Server CDC captured the change from the transaction log ───────────────────
    private static async Task Face2_CdcCaptureAsync(Config cfg, string code)
    {
        Face(2, "CDC capture", "SQL Server CDC recorded the change (cdc.dbo_Customers_CT) from the log");
        await using var conn = new SqlConnection(cfg.SqlConnection);
        await conn.OpenAsync().ConfigureAwait(false);

        // The capture job scans the log on a ~5s cadence — poll briefly until our row appears.
        var deadline = DateTime.UtcNow.AddSeconds(40);
        while (DateTime.UtcNow < deadline)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT TOP 1 __$operation, sys.fn_varbintohexstr(__$start_lsn) AS lsn
                FROM cdc.dbo_Customers_CT WHERE CustomerCode = @code ORDER BY __$start_lsn DESC;
                """;
            cmd.Parameters.AddWithValue("@code", code);
            await using var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            if (await r.ReadAsync().ConfigureAwait(false))
            {
                int op = r.GetInt32(0);
                Console.WriteLine($"  captured: operation={op} ({OperationName(op)})  start_lsn={r.GetString(1)}");
                Console.WriteLine("  → CDC read the change from the log — the OLTP tables were never queried by the pipeline.");
                return;
            }
            await Task.Delay(2000).ConfigureAwait(false);
        }
        throw new InvalidOperationException("CDC did not capture the change within 40s.");
    }

    // ── Face 3 ─ Debezium streamed the change to Kafka as a JSON CDC envelope ──────────────────
    private static async Task<RawChange> Face3_DebeziumRawAsync(Config cfg, string code)
    {
        Face(3, "Debezium → Kafka (raw)", $"the CDC event on topic {RawTopic} (JSON), over mTLS");
        var consumerConfig = KafkaClientFactory.CreateConsumerConfig(cfg.Kafka, "dfs-trace-raw-" + Guid.NewGuid());
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(RawTopic);

        var deadline = DateTime.UtcNow.AddSeconds(60);
        try
        {
            while (DateTime.UtcNow < deadline)
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(3));
                if (cr is null)
                {
                    continue;
                }

                using var doc = JsonDocument.Parse(cr.Message.Value);
                // JsonConverter (schemas.enable=false) emits the payload at the root; be tolerant of both.
                var payload = doc.RootElement.TryGetProperty("payload", out var p) ? p : doc.RootElement;
                if (!payload.TryGetProperty("after", out var after) || after.ValueKind == JsonValueKind.Null)
                {
                    continue;
                }
                if (after.GetProperty("CustomerCode").GetString() != code)
                {
                    continue;
                }

                var op = payload.TryGetProperty("op", out var opEl) ? opEl.GetString() ?? "?" : "?";
                long tsMs = payload.TryGetProperty("source", out var src) && src.TryGetProperty("ts_ms", out var ts) ? ts.GetInt64() : 0;
                var change = new RawChange(
                    after.GetProperty("CustomerId").GetInt64(),
                    code,
                    after.GetProperty("DisplayName").GetString() ?? "",
                    after.GetProperty("Email").GetString() ?? "",
                    after.GetProperty("Status").GetInt32(),
                    after.GetProperty("LifetimeValueUsd").GetString() ?? "0",
                    op,
                    tsMs);

                Console.WriteLine($"  key={cr.Message.Key}  op={op}  partition={cr.Partition.Value} offset={cr.Offset.Value}");
                Console.WriteLine($"  after={{ CustomerId:{change.CustomerId}, Code:{change.CustomerCode}, Name:\"{change.DisplayName}\", Email:{change.Email}, LTV:{change.LifetimeValueUsd} }}");
                Console.WriteLine("  → self-contained CDC envelope on the log broker; producers + consumers are fully decoupled.");
                return change;
            }
        }
        finally
        {
            consumer.Close();
        }
        throw new InvalidOperationException($"Did not see the change on {RawTopic} within 60s.");
    }

    // ── Face 4 ─ the .NET curation worker reshapes raw → clean Avro on a curated topic ─────────
    private static async Task Face4_CuratedAvroAsync(Config cfg, RawChange raw)
    {
        Face(4, "Curated Avro", $"reshape → domain event on {CuratedTopic}, Avro via Schema Registry");
        await EnsureTopicAsync(cfg, CuratedTopic).ConfigureAwait(false);

        var srOptions = new SchemaRegistryOptions { Url = cfg.SchemaRegistryUrl, EnableCertificateVerification = false };
        using var registry = AvroSerdes.CreateRegistryClient(srOptions);

        var schema = (RecordSchema)Avro.Schema.Parse(CuratedAvroSchema);
        var record = new GenericRecord(schema);
        record.Add("customerId", raw.CustomerId);
        record.Add("customerCode", raw.CustomerCode);
        record.Add("displayName", raw.DisplayName);
        record.Add("email", raw.Email);
        record.Add("status", raw.Status);
        record.Add("lifetimeValueUsd", raw.LifetimeValueUsd);
        record.Add("operation", raw.Operation == "r" ? "snapshot" : raw.Operation);
        record.Add("sourceTsMs", raw.SourceTsMs);
        record.Add("curatedAtUtc", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // Produce the curated Avro record — the AvroSerializer registers the schema under the
        // topic-value subject on first publish, so the message on the wire is self-describing.
        var producerConfig = KafkaClientFactory.CreateProducerConfig(cfg.Kafka);
        using (var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
                   .SetValueSerializer(AvroSerdes.CreateSerializer<GenericRecord>(registry))
                   .Build())
        {
            await producer.ProduceAsync(CuratedTopic,
                new Message<string, GenericRecord> { Key = raw.CustomerCode, Value = record }).ConfigureAwait(false);
            producer.Flush(TimeSpan.FromSeconds(10));
        }
        Console.WriteLine("  produced curated CustomerChanged (Avro) — schema registered in the Schema Registry.");

        // Read it back to prove it round-trips through Avro + the registry.
        var consumerConfig = KafkaClientFactory.CreateConsumerConfig(cfg.Kafka, "dfs-trace-curated-" + Guid.NewGuid());
        using (var consumer = new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                   .SetValueDeserializer(AvroSerdes.CreateDeserializer<GenericRecord>(registry).AsSyncOverAsync())
                   .Build())
        {
            consumer.Subscribe(CuratedTopic);
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(3));
                if (cr?.Message.Key != raw.CustomerCode)
                {
                    continue;
                }
                var v = cr.Message.Value;
                Console.WriteLine($"  decoded Avro: customerId={v["customerId"]}  code={v["customerCode"]}  operation={v["operation"]}  curatedAtUtc={v["curatedAtUtc"]}");
                break;
            }
            consumer.Close();
        }

        // Show the registered schema (id + JSON) — the contract the downstream consumers bind to.
        var subject = CuratedTopic + "-value";
        var latest = await registry.GetLatestSchemaAsync(subject).ConfigureAwait(false);
        Console.WriteLine($"  Schema Registry: subject={subject}  id={latest.Id}  version={latest.Version}");
        Console.WriteLine("  → downstream (StarRocks DWH, ClickHouse telemetry) reads this typed, versioned contract, not Debezium's raw shape.");
    }

    // ── Face 5 ─ the sink (Week-2: in-memory projection; StarRocks/ClickHouse arrive Week 3) ───
    private static void Face5_Sink(RawChange raw)
    {
        Face(5, "Sink", "materialize a projection (Week-3 wires StarRocks dwh + ClickHouse analytics)");
        Console.WriteLine($"  dim_customer upsert → sk=(surrogate)  customer_id={raw.CustomerId}  code={raw.CustomerCode}  is_current=true");
        Console.WriteLine("  → in Week 3 this becomes an SCD2 dimension load in StarRocks + a pipeline-telemetry row in ClickHouse.");
    }

    // Ensures a topic exists (brokers have auto-create off), matching the Debezium topic.creation fix.
    private static async Task EnsureTopicAsync(Config cfg, string topic)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = cfg.Kafka.BootstrapServers,
            SecurityProtocol = SecurityProtocol.Ssl,
            SslCaPem = cfg.Kafka.CaCertPem,
            SslCertificatePem = cfg.Kafka.ClientCertPem,
            SslKeyPem = cfg.Kafka.ClientKeyPem,
        };
        using var admin = new AdminClientBuilder(adminConfig).Build();
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }])
                .ConfigureAwait(false);
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Already created by a prior run — fine.
        }
    }

    private static string OperationName(int op) => op switch
    {
        1 => "delete",
        2 => "insert",
        3 => "update(before)",
        4 => "update(after)",
        _ => "unknown",
    };

    private static void Banner(string title, string subtitle)
    {
        Console.WriteLine();
        Console.WriteLine(new string('═', 78));
        Console.WriteLine($"  {title}");
        Console.WriteLine($"  {subtitle}");
        Console.WriteLine(new string('═', 78));
    }

    private static void Face(int n, string title, string what)
    {
        Console.WriteLine();
        Console.WriteLine($"── FACE {n} · {title} " + new string('─', Math.Max(0, 58 - title.Length)));
        Console.WriteLine($"   {what}");
    }

    // A minimal projection of the raw Debezium change we carry between faces.
    private sealed record RawChange(
        long CustomerId, string CustomerCode, string DisplayName, string Email,
        int Status, string LifetimeValueUsd, string Operation, long SourceTsMs);

    // All connection settings, resolved from environment variables (secrets never in code).
    private sealed record Config
    {
        public required string SqlConnection { get; init; }
        public required KafkaConnectionOptions Kafka { get; init; }
        public required string SchemaRegistryUrl { get; init; }

        public static Config FromEnvironment()
        {
            string Req(string k) => Environment.GetEnvironmentVariable(k)
                ?? throw new InvalidOperationException($"Missing environment variable {k}.");

            return new Config
            {
                SqlConnection = Req("DFS_SQL_CONN"),
                SchemaRegistryUrl = Environment.GetEnvironmentVariable("DFS_SR_URL") ?? "https://192.168.10.91:8081",
                Kafka = new KafkaConnectionOptions
                {
                    BootstrapServers = Environment.GetEnvironmentVariable("DFS_KAFKA_BOOTSTRAP")
                        ?? "192.168.10.21:9092,192.168.10.22:9092,192.168.10.23:9092",
                    CaCertPem = File.ReadAllText(Req("DFS_KAFKA_CA")),
                    ClientCertPem = File.ReadAllText(Req("DFS_KAFKA_CERT")),
                    ClientKeyPem = File.ReadAllText(Req("DFS_KAFKA_KEY")),
                },
            };
        }
    }
}
