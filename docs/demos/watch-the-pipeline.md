# Watch the DataFlow Studio pipeline by hand

Follow one customer record across every face of the pipeline using everyday tools — SSMS for SQL,
`ssh` + the Kafka console tools for the stream, `curl` for Debezium, `clickhouse-client` for the
telemetry. Every endpoint, port, and credential is listed so you can reproduce each hop yourself.

Faces 1–5 are the **data** path (OLTP write → CDC → raw Debezium → curated Avro → the StarRocks
star). Face 6 is the **observability** path: the pipeline reporting on itself.

> **Prereqs:** SQL AG + `kafka-east` + `schema-registry` + `kafka-connect` powered on, the Debezium
> `oltp-cdc` connector running, Vault unsealed. Face 5 additionally needs StarRocks; Face 6 needs
> ClickHouse. The one-command version of the Faces 1–5 flow is `.\scripts\dfs-trace.ps1`.

---

## 0. Get the credentials you'll need

The SQL `sa` password lives in Vault. From PowerShell on the build host:

```powershell
$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token
vault kv get -field=content nexus/oltp/sqlserver/sa-password    # copy this for SSMS
```

The lab SSH key is `~/.ssh/nexus_gateway_ed25519`, user `nexusadmin`.

---

## Face 1 — OLTP write (SQL Server Management Studio)

**Connect:**

| Field | Value |
|---|---|
| Server name | `192.168.70.16,1433`  (the FCI virtual; or the AG Listener `192.168.70.17,1433`) |
| Authentication | **SQL Server Authentication** |
| Login | `sa` |
| Password | *(from step 0)* |
| Encryption | **Mandatory** |
| Trust server certificate | ✅ **check this** (the cert is from the lab PKI, not a public CA) |

**See the data:** expand `Databases → OltpDb → Tables → dbo.Customers`, right-click → *Select Top 1000 Rows*. Or:

```sql
SELECT TOP 10 CustomerId, CustomerCode, DisplayName, Email, LifetimeValueUsd, created_utc, row_version
FROM OltpDb.dbo.Customers ORDER BY CustomerId DESC;
```

**Insert one yourself** (this is Face 1 of the trace):

```sql
INSERT INTO OltpDb.dbo.Customers (CustomerCode, DisplayName, Email, PreferredLocale, Status, LifetimeValueUsd, created_by, modified_by)
VALUES ('MANUAL-001', 'Grace Hopper', 'grace@example.com', 'en-US', 1, 100.00, 'greg', 'greg');
```

---

## Face 2 — CDC capture (still in SSMS)

SQL Server captured that insert from the transaction log into a change table. Query it:

```sql
SELECT __$operation AS op,           -- 1=delete 2=insert 3=update(before) 4=update(after)
       sys.fn_varbintohexstr(__$start_lsn) AS start_lsn,
       CustomerCode, DisplayName, Email
FROM OltpDb.cdc.dbo_Customers_CT
ORDER BY __$start_lsn DESC;
```

You should see your `MANUAL-001` row with `op=2`. (There's a ~5s capture-job delay.)

---

## Debezium — is the connector healthy?

From the build host (bash), ask Kafka Connect's REST API over SSH:

```bash
ssh -i ~/.ssh/nexus_gateway_ed25519 nexusadmin@192.168.70.95 \
  "curl -sk https://localhost:8083/connectors/oltp-cdc/status"
```

Expect `"state":"RUNNING"` for the connector and task 0. (`/connectors` lists all; `/connectors/oltp-cdc/topics` lists what it has written to.)

---

## Face 3 — Debezium → Kafka, the raw CDC event

The stream is mTLS-only, so the consumer needs a client config. SSH to a broker and create it once
(the certs already live on the node), then consume the topic:

```bash
ssh -i ~/.ssh/nexus_gateway_ed25519 nexusadmin@192.168.70.21

# on the node — build a client config that presents the node's cert (a super-user):
cat > /tmp/client.properties <<'EOF'
security.protocol=SSL
ssl.keystore.type=PEM
ssl.keystore.location=/etc/nexus-kafka/tls/keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=/etc/nexus-kafka/tls/truststore.pem
ssl.endpoint.identification.algorithm=https
EOF

# tail the raw CDC topic (JSON) — leave it running, then insert a row in SSMS to watch it appear:
sudo /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 192.168.10.21:9092 \
  --consumer.config /tmp/client.properties \
  --topic oltp.OltpDb.dbo.Customers --from-beginning
```

Each message is a Debezium envelope: `{"before":..., "after":{...}, "op":"c", "source":{...}}`. Pipe
through `python3 -m json.tool` for pretty output, or just eyeball the `after` block.

**List the pipeline topics:**

```bash
sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server 192.168.10.21:9092 \
  --command-config /tmp/client.properties --list | grep -E '^oltp|^dfs|schemahistory'
```

---

## Face 4 — the curated Avro event + Schema Registry

The curated topic `dfs.customers.changed.v1` carries schema-registered **Avro** (binary), so a plain
console consumer shows bytes. Two ways to read it decoded:

**A) The `dfs-trace` tool** (easiest — it produces + decodes + prints the schema):

```powershell
.\scripts\dfs-trace.ps1
```

**B) Confluent's Avro console consumer** (on a broker node) — decodes against the registry:

```bash
sudo /opt/confluent/bin/kafka-avro-console-consumer \
  --bootstrap-server 192.168.10.21:9092 \
  --consumer.config /tmp/client.properties \
  --topic dfs.customers.changed.v1 --from-beginning \
  --property schema.registry.url=https://192.168.10.91:8081 \
  --property schema.registry.ssl.endpoint.identification.algorithm=
```

**Inspect the registered schema** directly on a Schema Registry node:

```bash
ssh -i ~/.ssh/nexus_gateway_ed25519 nexusadmin@192.168.70.91 \
  "curl -sk https://localhost:8081/subjects; echo; \
   curl -sk https://localhost:8081/subjects/dfs.customers.changed.v1-value/versions/latest"
```

You'll see the subject, its schema id/version, and the Avro JSON — the contract downstream binds to.

---

## Face 5 — the sink (StarRocks Kimball DWH)

The curated topics are the DWH's source. Two commands take you from an empty star to a queryable one
(both idempotent — run them twice and nothing doubles):

```powershell
.\scripts\dfs-curate.ps1            # raw CDC -> curated Avro on all 10 dfs.*.changed.v1 topics
.\scripts\dfs-warehouse-sink.ps1    # curated Avro -> StarRocks dwh (SCD2 dims + facts)
```

`dfs-curate` prints a per-entity count (59 on a fresh seed); `dfs-warehouse-sink` prints what it
loaded. Now query the star — **DataGrip** (*New → MySQL*, host `192.168.70.31`, port `9030`, user
`root`) or on the FE node:

```bash
# WHERE: sr-fe-leader (192.168.70.31) — the StarRocks MySQL wire is TLS-off, so --skip-ssl
mysql -h127.0.0.1 -P9030 -uroot -p<starrocks-root> --skip-ssl -e "
  SELECT customer_sk, customer_id, display_name, valid_from, valid_to, is_current
  FROM dwh.dim_customer WHERE customer_code='SEED-C001';"
```

That's the **SCD2 dimension**: one row per version, `is_current=1` on the live one. Now the star —
a fact joined to its dimensions:

```sql
SELECT c.display_name, p.display_name AS product, l.quantity, l.line_total_usd, d.full_date
FROM dwh.fact_order_line l
JOIN dwh.dim_customer c ON l.customer_sk = c.customer_sk AND c.is_current = 1
JOIN dwh.dim_product  p ON l.product_sk  = p.product_sk  AND p.is_current = 1
JOIN dwh.dim_date     d ON l.order_date_key = d.date_key
ORDER BY l.order_line_id;
```

**Watch SCD2 actually version.** Change a seeded customer in SSMS, then re-run the two scripts:

```sql
UPDATE dbo.Customers SET DisplayName = 'Ada Lovelace (Countess)', modified_by = 'demo'
WHERE CustomerCode = 'SEED-C001';
```

Re-query `dwh.dim_customer WHERE customer_code='SEED-C001'` — now **two** rows: the old version
closed (`is_current=0`, `valid_to` stamped) and the new one current. That is the whole point of the
pipeline: an OLTP edit became a *versioned* warehouse fact, with the history preserved.

---

## Face 6 — the pipeline watching itself (ClickHouse)

Everything above was the *data* path. While it ran, both engines were also reporting on themselves —
and ClickHouse pulled that telemetry out of Kafka **on its own**, with no .NET consumer involved
(ADR-0008). Connect to any ClickHouse data node:

```bash
ssh -i ~/.ssh/nexus_gateway_ed25519 nexusadmin@192.168.70.44
clickhouse-client --host localhost --secure --accept-invalid-certificate
```

**How long did each stage take?**

```sql
SELECT pipeline, stage, count() AS n, round(avg(duration_ms),1) AS avg_ms
FROM analytics.pipeline_events
GROUP BY pipeline, stage ORDER BY pipeline, stage;
```

You'll see two pipelines: `curation` (one row per curated record, plus a `drain` run-summary) and
`warehouse-sink` (one row per loader stage — `dim_customer`, `fact_order`, …).

**How fresh is the data?** Every curated record carries a lag sample — `now` minus the moment SQL
Server committed the change:

```sql
SELECT source, count() AS n, round(min(lag_seconds),1) AS min_lag, round(max(lag_seconds),1) AS max_lag
FROM analytics.cdc_lag_seconds GROUP BY source;
```

Expect a wide spread, and that's correct: an edit you just made in Face 1 reads a few tens of
seconds, while records replayed from an older session honestly report their real age. Lag describes
the *event*, not the run.

**Percentiles, cheaply.** `pipeline_latency_by_hour` stores partial aggregate *states*, finalized at
read time with the `-Merge` combinators:

```sql
SELECT stage, countMerge(events_state) AS events,
       round(quantilesMerge(0.5,0.95,0.99)(p_state)[1],1) AS p50,
       round(quantilesMerge(0.5,0.95,0.99)(p_state)[2],1) AS p95,
       round(quantilesMerge(0.5,0.95,0.99)(p_state)[3],1) AS p99
FROM cluster('nexus_analytics', analytics.pipeline_latency_by_hour)
GROUP BY stage ORDER BY events DESC;
```

**Where did it come from?** Nothing in .NET wrote these rows. The workers produced JSON to
`dfs.telemetry.*`, and ClickHouse's own Kafka-engine tables consumed it:

```sql
SELECT name, engine FROM system.tables
WHERE database = 'analytics' AND (name LIKE '%_kafka' OR name LIKE '%_kafka_mv');
```

Six objects: three `Kafka` readers and three `MaterializedView` triggers that reshape each row into
its destination table. That is the engine doing the ETL.

---

## GUI alternative for Kafka (optional)

If you prefer a GUI over the console tools, **Offset Explorer** (Kafka Tool) or **Conduktor** can
connect with the same mTLS material: point them at `192.168.10.21:9092`, security = SSL, and import
`.secrets\kafka-client.crt` + `kafka-client.key` + `kafka-ca.crt` (the files `dfs-trace.ps1` writes)
as the keystore/truststore. The console commands above are the reliable, always-works path.
