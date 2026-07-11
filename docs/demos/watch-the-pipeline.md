# Watch the DataFlow Studio pipeline by hand

Follow one customer record across all five faces using everyday tools — SSMS for SQL,
`ssh` + the Kafka console tools for the stream, `curl` for Debezium. Every endpoint, port, and
credential is listed so you can reproduce each hop yourself.

> **Prereqs:** SQL AG + `kafka-east` + `schema-registry` + `kafka-connect` powered on, the Debezium
> `oltp-cdc` connector running, Vault unsealed. The one-command version of this whole flow is
> `.\scripts\dfs-trace.ps1`.

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

## Face 5 — the sink

Week 2 stops at a projection printed by the trace tool. In **Week 3** this becomes an SCD2 dimension
load in **StarRocks** (`dwh.dim_customer`) and a pipeline-telemetry row in **ClickHouse**
(`analytics.pipeline_events`) — you'll query those in DataGrip / the ClickHouse client.

---

## GUI alternative for Kafka (optional)

If you prefer a GUI over the console tools, **Offset Explorer** (Kafka Tool) or **Conduktor** can
connect with the same mTLS material: point them at `192.168.10.21:9092`, security = SSL, and import
`.secrets\kafka-client.crt` + `kafka-client.key` + `kafka-ca.crt` (the files `dfs-trace.ps1` writes)
as the keystore/truststore. The console commands above are the reliable, always-works path.
