# dataflow-studio — Operator Handbook

How to replay the whole pipeline from zero on the NexusPlatform lab: **OltpDb → SQL Server CDC →
Debezium → curated Avro → StarRocks DWH**. Every command here has been run live; §3.2 is the ledger
of everything that actually bit us, with the fix.

Unlike the `nexus-infra-*` handbooks this is an **application** handbook — it does not build VMs. It
assumes the infra tiers exist (they are built by their own repos) and shows how to bring the pipeline
up on them, in order, and verify it.

---

## §0 Prerequisites

**Build host** (Windows, WORKGROUP — no `nexus.lab` DNS, so everything is addressed by IP):

| Need | Where |
|---|---|
| .NET SDK | `net10.0` (see `global.json`) |
| Vault CLI | `%LOCALAPPDATA%\Microsoft\WinGet\Links\vault.exe` |
| `sqlcmd` | `C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn` |
| GitHub Packages token | `gh auth token` → `GITHUB_PACKAGES_TOKEN` (restores the `Nexus.*` family) |
| SSH key to lab nodes | `~/.ssh/nexus_gateway_ed25519` (`nexusadmin@<ip>`) |
| Docker | only for the CI/test gates (Testcontainers), not for the lab |

**Vault** (the source of every credential — nothing is hard-coded):

```powershell
$env:VAULT_ADDR   = 'https://192.168.70.121:8200'
$env:VAULT_CACERT = "$HOME\.nexus\vault-ca-bundle.crt"
$env:VAULT_TOKEN  = (Get-Content "$HOME\.nexus\vault-init.json" -Raw | ConvertFrom-Json).root_token
```

> The on-disk `~/.vault-token` is the low-privilege smoke token — use the **root token** from
> `vault-init.json`. Windows `curl` + schannel mis-handles the IP-SAN cert; use the `vault` CLI.

| Credential | Command |
|---|---|
| SQL `sa` | `vault kv get -field=content nexus/oltp/sqlserver/sa-password` |
| StarRocks `root` | `vault kv get -field=password nexus/analytics/starrocks/root-password` |
| ClickHouse `admin` | `vault kv get -field=password nexus/analytics/clickhouse/admin-password` |
| Kafka client cert | `vault write -format=json pki_int/issue/kafka-broker common_name=localhost ttl=24h` |

**Lab tiers** — power on only what the step needs (minimal-running-VMs); base 6 always run
(`nexus-gateway`, `dc-nexus`, `vault-1/2/3`, `vault-transit`).

| Step | Tier | Nodes |
|---|---|---|
| seed / CDC | SQL AG | `sql-fci-1/2` `.11/.12`, `sql-ag-rep-1/2` `.13/.14` (OltpDb on FCI **`.16`**) |
| CDC → raw | Kafka | `kafka-east-1/2/3` `.21-.23`, `schema-registry-1/2` `.91/.92`, `kafka-connect-1/2` `.95/.96` |
| DWH sink | StarRocks (shared-**nothing**) | `sr-fe-leader` `.31` + `sr-fe-follower-1/2` `.32/.33`, `sr-be-1/2/3` `.34-.36` |
| telemetry | ClickHouse | `ch-keeper-1/2/3` `.41-.43`, `ch-shard{1,2,3}-rep{1,2}` `.44-.49` (cluster **`nexus_analytics`**) |

---

## §1 From-zero replay

### 1.1 Build

```powershell
$env:GITHUB_PACKAGES_TOKEN = (gh auth token)   # Nexus.* comes from GitHub Packages
dotnet build DataFlowStudio.slnx -c Release    # expect 0 warnings / 0 errors
dotnet test  DataFlowStudio.slnx -c Release    # unit + architecture + migration gates
```

The migration gates spin throwaway containers (SQL Server, `starrocks/allin1-ubuntu:3.5.17`,
`clickhouse-server`) — they need Docker but **not** the lab.

### 1.2 Migrate the three schemas

Each store has its own tool (ADR-0005). Run against the live tiers:

```powershell
# OltpDb (SQL Server) — FluentMigrator, reversible (E1 up -> down -> up)
$env:DFS_OLTP_CONNECTION = "Server=192.168.70.16,1433;Database=OltpDb;User Id=sa;Password=<sa>;Encrypt=True;TrustServerCertificate=True"
dotnet run --project src/DataFlowStudio.Migrations.Oltp -c Release -- up

# StarRocks dwh + analytics — DbUp, forward-only, idempotent
$env:DFS_STARROCKS_CONNECTION = "Server=192.168.70.31;Port=9030;User ID=root;Password=<sr>;SslMode=None;AllowPublicKeyRetrieval=true"
dotnet run --project src/DataFlowStudio.Migrations.Starrocks -c Release -- up

# ClickHouse analytics — DbUp-pattern runner (needs the root+INTERMEDIATE CA — see §3.2 T6)
$env:DFS_CLICKHOUSE_CONNECTION = "Host=192.168.70.44;Port=8443;Protocol=https;Username=admin;Password=<ch>;Database=default"
$env:DFS_CLICKHOUSE_CACERT     = "<path>\nexus-ca-chain.crt"
dotnet run --project src/DataFlowStudio.Migrations.Clickhouse -c Release -- up
```

Re-running any of them is a **no-op** (journal tables). Expected: `dwh` = 5 dims + 4 facts +
`bridge_customer_seg` + `schemaversions`; `analytics` (StarRocks) = `dim_customer_current` view;
`analytics` (ClickHouse) = `pipeline_events(_local)`, `pipeline_latency_by_hour`, `cdc_lag_seconds`,
`error_events` + `dfs_schema_versions`.

> **Order matters for ClickHouse.** `Script0005` creates **Kafka-engine** source tables that start
> consuming the moment they exist, so do **§1.5a first** (the ClickHouse-as-Kafka-client setup) — a
> table created before the `<kafka>` config just logs connection errors until it lands. On a
> single-node CI container `Script0005` is skipped entirely (`ExcludedScripts`, ADR-0008).

### 1.3 Enable CDC + point Debezium at all ten tables

CDC must be on **every** table Debezium captures (verify, don't assume):

```powershell
sqlcmd -S 192.168.70.16,1433 -U sa -P <sa> -d OltpDb -C -N -Q `
  "SELECT name, is_tracked_by_cdc FROM sys.tables WHERE type='U' AND schema_id=SCHEMA_ID('dbo') ORDER BY name"
```

The `oltp-cdc` connector must include all ten order-flow tables and the right encodings
(**`decimal.handling.mode=string`**, **`time.precision.mode=connect`** — the curation catalog depends
on both). From `kafka-connect-1`:

```bash
TABLES="dbo.Customers,dbo.ProductCategories,dbo.Products,dbo.Warehouses,dbo.CustomerAddresses,dbo.Orders,dbo.OrderLines,dbo.Transactions,dbo.Shipments,dbo.ProductInventory"
cfg=$(curl -sk https://localhost:8083/connectors/oltp-cdc/config)
echo "$cfg" | jq -c ".[\"table.include.list\"]=\"$TABLES\" | .[\"time.precision.mode\"]=\"connect\"" \
  | curl -sk -X PUT -H 'Content-Type: application/json' https://localhost:8083/connectors/oltp-cdc/config -d @-
curl -sk https://localhost:8083/connectors/oltp-cdc/status     # expect RUNNING / RUNNING
```

### 1.4 Seed OltpDb

```powershell
.\scripts\dfs-seed.ps1
```

Idempotent (marker `SEED-C001`). Inserts 4 categories, 6 products, 3 warehouses, 4 customers +
addresses, 18 inventory rows, 4 orders, 6 lines, 4 transactions, 2 shipments. Because the connector
is already watching, these arrive as **streaming** CDC (`op=c`) — no re-snapshot needed.

Verify the raw topics filled (from a broker):

```bash
for t in Customers Products Orders OrderLines Transactions Shipments ProductInventory ProductCategories Warehouses CustomerAddresses; do
  sudo /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 192.168.10.21:9092 \
    --command-config /etc/nexus-kafka/client-ssl.properties --topic "oltp.OltpDb.dbo.$t"
done
```

### 1.5 Kafka ACLs

Brokers enforce ACLs (only broker principals are super-users). The app principal is
`User:CN=localhost`. Grant once, from a broker:

```bash
sudo /opt/kafka/bin/kafka-acls.sh --bootstrap-server 192.168.10.21:9092 \
  --command-config /etc/nexus-kafka/client-ssl.properties \
  --add --allow-principal 'User:CN=localhost' --operation READ --group 'dfs-curation' --resource-pattern-type prefixed
```

Already granted (Week 2): topic-prefix `oltp` READ/DESCRIBE; topic-prefix `dfs`
READ/WRITE/CREATE/DESCRIBE; cluster CREATE/DESCRIBE/IDEMPOTENT_WRITE; group-prefix `dfs-trace` READ.
Added in 3D: group-prefix **`dfs-warehouse-sink`** READ (the sink now uses its own group — it no
longer borrows `dfs-curation`). The `dfs` topic prefix already covers the `dfs.telemetry.*` topics,
so the producers need no new topic grant.

### 1.5a ClickHouse as a Kafka client — native telemetry ingestion (ADR-0008)

ClickHouse pulls telemetry from Kafka itself, so it needs its own identity, mTLS material and ACLs.
Do this **once per rebuild**, before the ClickHouse migration applies `Script0005`.

**1 — a dedicated PKI role + client cert.** The shared `kafka-broker` role has `allow_any_name=false`
and does not list this CN; do **not** patch it (a partial `vault write` resets a role's other fields).
Create a separate role instead:

```bash
vault write pki_int/roles/kafka-clickhouse-client \
  allowed_domains="clickhouse-telemetry" allow_bare_domains=true allow_subdomains=false \
  allow_any_name=false allow_localhost=false server_flag=false client_flag=true \
  key_usage="DigitalSignature,KeyAgreement,KeyEncipherment" ext_key_usage="ClientAuth" \
  max_ttl=720h ttl=168h

vault write -format=json pki_int/issue/kafka-clickhouse-client \
  common_name=clickhouse-telemetry ttl=168h > issued.json
```

Extract `certificate` / `private_key` with a real JSON parser (**never** `grep`/`sed` — §3.2 T11).
`ca.crt` is the **root-only** `~/.nexus/vault-ca-bundle.crt`: the brokers send their full chain, so
root alone validates them (unlike ClickHouse's own HTTPS listener — §3.2 T6).

**2 — place the material + config on all six data nodes** (`.44`–`.49`):

```bash
sudo install -d -o root -g clickhouse -m 0750 /etc/clickhouse-server/kafka
sudo install -o root -g clickhouse -m 0640 client.crt client.key ca.crt /etc/clickhouse-server/kafka/
sudo install -o root -g clickhouse -m 0640 kafka-telemetry.xml /etc/clickhouse-server/config.d/
sudo systemctl restart nexus-clickhouse-server     # rolling, one node at a time
```

`/etc/clickhouse-server/config.d/kafka-telemetry.xml`:

```xml
<clickhouse>
    <kafka>
        <security_protocol>ssl</security_protocol>
        <ssl_ca_location>/etc/clickhouse-server/kafka/ca.crt</ssl_ca_location>
        <ssl_certificate_location>/etc/clickhouse-server/kafka/client.crt</ssl_certificate_location>
        <ssl_key_location>/etc/clickhouse-server/kafka/client.key</ssl_key_location>
    </kafka>
</clickhouse>
```

TLS lives here, never in the DDL. Broker certs carry the backplane IP SAN (`192.168.10.21`), so
hostname verification passes without weakening it.

**3 — ACLs for the new principal + the telemetry topics** (from a broker):

```bash
sudo /opt/kafka/bin/kafka-acls.sh --bootstrap-server 192.168.10.21:9092 \
  --command-config /etc/nexus-kafka/client-ssl.properties \
  --add --allow-principal 'User:CN=clickhouse-telemetry' \
  --operation READ --operation DESCRIBE --topic dfs.telemetry --resource-pattern-type prefixed

sudo /opt/kafka/bin/kafka-acls.sh --bootstrap-server 192.168.10.21:9092 \
  --command-config /etc/nexus-kafka/client-ssl.properties \
  --add --allow-principal 'User:CN=clickhouse-telemetry' \
  --operation READ --group dfs-clickhouse --resource-pattern-type prefixed

for t in pipeline_events cdc_lag error_events; do
  sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server 192.168.10.21:9092 \
    --command-config /etc/nexus-kafka/client-ssl.properties \
    --create --if-not-exists --topic "dfs.telemetry.$t" --partitions 1 --replication-factor 3
done
```

Creating the topics up front keeps ClickHouse from logging "unknown topic" while it waits (the
workers would otherwise create them on first run).

### 1.6 Curate — raw CDC → curated Avro

```powershell
.\scripts\dfs-curate.ps1
```

Drains every raw topic, reshapes each change per the curation catalog (ADR-0007), and produces
`dfs.<entity>.changed.v1` Avro through the Schema Registry. Expected on a fresh seed: **59 curated
records** across all ten topics, and ten `dfs.*-value` subjects registered.

### 1.7 Load the StarRocks DWH

```powershell
.\scripts\dfs-warehouse-sink.ps1
```

Consumes the curated snapshot and loads the star (ADR-0006): SCD2 `dim_customer`/`dim_product`,
SCD1 `dim_warehouse`/`dim_carrier`, generated `dim_date`, then the four facts. Idempotent — a re-run
adds no versions and no rows.

### 1.8 Verify the star

```sql
-- on sr-fe-leader:  mysql -h127.0.0.1 -P9030 -uroot -p<sr> --skip-ssl
SELECT 'dim_customer(cur)', COUNT(*) FROM dwh.dim_customer WHERE is_current=1
UNION ALL SELECT 'dim_product(cur)', COUNT(*) FROM dwh.dim_product WHERE is_current=1
UNION ALL SELECT 'fact_order', COUNT(*) FROM dwh.fact_order
UNION ALL SELECT 'fact_order_line', COUNT(*) FROM dwh.fact_order_line
UNION ALL SELECT 'fact_transaction', COUNT(*) FROM dwh.fact_transaction
UNION ALL SELECT 'fact_inventory_snap', COUNT(*) FROM dwh.fact_inventory_snap;

-- the star, joined:
SELECT c.display_name, o.order_id, o.total_usd, d.full_date
FROM dwh.fact_order o
JOIN dwh.dim_customer c ON o.customer_sk = c.customer_sk AND c.is_current = 1
JOIN dwh.dim_date     d ON o.order_date_key = d.date_key
ORDER BY o.order_id;
```

Expected: dims 8/6/3/2, `dim_date` 5, facts 4/6/4/18, and four orders with customer names + dates.

### 1.8a Verify the telemetry (ClickHouse, native ingestion)

Both engines instrument themselves, so §1.6 and §1.7 already produced telemetry to `dfs.telemetry.*`
and ClickHouse ingested it through the Kafka-engine tables. Give it a few seconds to poll, then:

```sql
-- on any CH data node:  clickhouse-client --secure --accept-invalid-certificate
SELECT pipeline, stage, count() AS n, round(avg(duration_ms),1) AS avg_ms
FROM analytics.pipeline_events GROUP BY pipeline, stage ORDER BY pipeline, stage;

SELECT source, count() AS n, round(min(lag_seconds),1) AS min_lag, round(max(lag_seconds),1) AS max_lag
FROM analytics.cdc_lag_seconds GROUP BY source;

-- the AggregatingMergeTree rollup. The MV has no Distributed wrapper, so read it via cluster().
SELECT stage,
       countMerge(events_state) AS events,
       round(quantilesMerge(0.5,0.95,0.99)(p_state)[1],1) AS p50,
       round(quantilesMerge(0.5,0.95,0.99)(p_state)[2],1) AS p95,
       round(quantilesMerge(0.5,0.95,0.99)(p_state)[3],1) AS p99
FROM cluster('nexus_analytics', analytics.pipeline_latency_by_hour)
GROUP BY stage ORDER BY events DESC;

SELECT event_time, trace_id, error_code, message FROM analytics.error_events ORDER BY event_time DESC;
```

Expected after one curate + one sink: `pipeline_events` carries **two** pipelines — `curation` (one
row per curated record, plus a `drain` run-summary) and `warehouse-sink` (one row per loader stage:
`dim_*`, `fact_*`, plus a `load` summary) — and the MV returns real p50/p95/p99 per stage.

`cdc_lag_seconds` gets one sample per curated record. Expect a **wide spread**, and that is correct:
freshly-issued source changes read a few tens of seconds, while records still sitting on the raw
topics from an earlier session read their true age (days). Lag measures the *event*, not the run.

To see both error paths, inject one of each — natively through Kafka, and through the .NET
control path (`ClickHouseErrorSink`, used when the broker is unreachable):

```bash
echo '{"event_ms":'"$(( $(date +%s) * 1000 ))"',"trace_id":"native-proof","service":"curation",
"error_code":"demo-native-path","message":"native Kafka-engine error ingestion proof","stack":""}' |
sudo /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server 192.168.10.21:9092 \
  --producer.config /etc/nexus-kafka/client-ssl.properties --topic dfs.telemetry.error_events
```

### 1.9 Tear down

Stop the tiers back to base 6 (`vmrun stop <vmx> soft`). Nothing is destroyed: OltpDb + CDC, the raw
and curated Kafka topics, and the DWH all survive a power-off and resume on power-on.

---

## §2 Phase status

| Slice | State |
|---|---|
| Week 1 — scaffold · 11-table OltpDb migrations · E1 gate | ✅ shipped |
| Week 2 — CDC → Kafka: Debezium raw → curated Avro · 5-face trace | ✅ live |
| Week 3A — DbUp sink schema (StarRocks + ClickHouse) | ✅ live |
| Week 3B — curation for all 10 order-flow entities · seed tool | ✅ live |
| Week 3C — StarRocks DWH sink (SCD2 dims + facts) | ✅ live |
| Week 3D — ClickHouse telemetry sink (Kafka-engine native) | ✅ live |
| Week 3E — Marquez (OpenLineage) + observability tier | ⏳ next |
| Week 3F — `dfs trace` Face 5 real · Week-3 PR | ⏳ |

---

## §3 Operator runbooks

### 3.1 From-zero replay (the canonical sequence)

Build → migrate (OltpDb, StarRocks, ClickHouse) → CDC + connector → seed → ACLs → curate → sink →
verify → tear down. Power tiers on per step and stop them after:

```
§1.1  build                    (no lab)
§1.5a ClickHouse Kafka client  Kafka + ClickHouse   (before the CH migration — Script0005)
§1.2  migrate                  SQL AG · StarRocks · ClickHouse
§1.3  CDC + connector          SQL AG + Kafka
§1.4  seed                     SQL AG + Kafka
§1.5  ACLs                     Kafka
§1.6  curate                   Kafka + ClickHouse   (SQL AG may be stopped — raw topics persist;
                                                     ClickHouse must be up to ingest telemetry)
§1.7  sink                     Kafka + StarRocks + ClickHouse
§1.8  verify the star          StarRocks
§1.8a verify the telemetry     ClickHouse
§1.9  tear down                -> base 6
```

Full concurrency (all tiers at once) is ~245 GB on a 256 GB host — don't; sequence per step.

### 3.2 Transient ledger — what actually bit us, and the fix

**Authored-DDL defects** (the schema was paper-only until 3A; all four caught by the container gate
before touching the lab, and fixed in `schemas/dataflow-studio/README.md`):

| # | Symptom | Fix |
|---|---|---|
| T1 | PK-model dims distributed by a business key | StarRocks requires `DISTRIBUTED BY` ⊆ PRIMARY KEY → distribute by the **surrogate** key |
| T2 | `fact_order_line`(32) + `fact_transaction`(16) in one colocation group | a colocation group needs **identical bucket counts** → both 32 |
| T3 | *"Key columns must be the first few columns of the schema"* | `bridge_customer_seg` had `weight` between key columns → reorder keys first |
| T4 | ClickHouse DDL named `ON CLUSTER nexus_ch` | the lab cluster is **`nexus_analytics`** (Guide 13) → injected via the profile |

**Connectivity / tooling:**

| # | Symptom | Fix |
|---|---|---|
| T5 | `MissingMethodException` from `Testcontainers.MsSql` | **all** Testcontainers packages must share one core version (4.13.0) |
| T6 | ClickHouse TLS: chain = `PartialChain` | `~/.nexus/vault-ca-bundle.crt` is **root-only** and CH sends only its leaf → append `vault read -field=certificate pki_int/cert/ca` to build a root+intermediate bundle. **No client cert needed** on `:8443` |
| T7 | *"server returned compressed result but HttpClient did not decompress it"* | a custom `HttpClient` must set `AutomaticDecompression` |
| T8 | on-node `clickhouse-client --secure` to `localhost` → cert verify failed | add `--accept-invalid-certificate` for local checks |
| T9 | StarRocks MySQL wire | **TLS-off** → `--skip-ssl` (CLI) / `SslMode=None` (MySqlConnector) |
| T10 | PowerShell `"https://$node:8443/"` → *"hostname could not be parsed"* | scope-qualifier — use `"https://${node}:8443/"` |
| T11 | Broker: `tlsv13 alert certificate required` | the Vault cert was extracted with `grep`/`sed` and came out **empty** → extract with pwsh `ConvertFrom-Json` + `Set-Content -NoNewline` (no python on the build host) |
| T12 | `Failed to acquire idempotence PID … Coordinator load in progress` | transient right after broker power-on; it retries |

**Pipeline semantics:**

| # | Symptom | Fix |
|---|---|---|
| T13 | Debezium captured only `dbo.Customers` | the Week-2 connector was demo-scoped → `table.include.list` = all 10 + `time.precision.mode=connect` |
| T14 | Schema Registry 409: *`preferredLocale` … has no default value* | adding a field breaks BACKWARD compat without a default → curated Avro fields now all carry **defaults** (customers evolved to a clean v2) |
| T15 | *"Source column 'LineTotalUsd' is missing/null"* | it is a **computed PERSISTED** column and SQL Server CDC stores computed columns as NULL → not carried; the DWH loader recomputes `qty*price - discount` |
| T16 | Inventory loaded 6 rows instead of 18 | `dfs.product-inventory.changed.v1` is keyed by `productId` alone but its grain is product+warehouse → the sink dedups it by the **composite** key |
| T17 | Duplicate curated records | at-least-once delivery is normal → every sink path is idempotent (SCD2 skips unchanged; facts truncate-reload) |
| T18 | StarRocks "too many versions" risk | never insert row-by-row → each loader emits **one batched multi-row INSERT** per table |
| T19 | Facts are `PARTITION BY RANGE(date_key)()` with no partitions | the loader runs `ADD PARTITION IF NOT EXISTS p<dk> VALUES [("dk"),("dk+1"))` for each date key in the batch |

**Native telemetry ingestion (3D):**

| # | Symptom | Fix |
|---|---|---|
| T20 | `vault write pki_int/issue/kafka-broker common_name=clickhouse-telemetry` rejected | the role has `allow_any_name=false` and does not list that CN. Do **not** patch the shared role — a partial `vault write` resets its other fields → create a separate `kafka-clickhouse-client` role (§1.5a) |
| T21 | Kafka-engine `DateTime64` ingestion is fragile via datetime **strings** | `date_time_input_format` is a server/format setting, not per-table → cross the wire as epoch-ms `event_ms Int64` and convert in the MV with `fromUnixTimestamp64Milli` (ADR-0008) |
| T22 | `Script0005` cannot run on a broker-less CI container | `ClickHouseMigrationProfile.ExcludedScripts` — the `SingleNode` profile skips it (never applied, never journalled); the E1 gate still asserts the rest of the schema |
| T23 | `dotnet format` "Unable to fix IDE1006 … doesn't support Fix All" | the naming fixer cannot batch-rename private fields → either prefix them `_` by hand or (preferred, and what the codebase does elsewhere) use a **primary constructor** so they are parameters, not fields |
| T24 | `ConsumeException: FindCoordinator … Group authorization failed` on the DWH sink | the `dfs-warehouse-sink` group had no ACL (3C borrowed `dfs-curation`) → granted group-prefix `dfs-warehouse-sink` READ; the borrow is retired |
| T25 | `SHOW BACKENDS` over `mysql -uroot` hangs / returns nothing | StarRocks `root` **has a password** (`nexus/analytics/starrocks/root-password`); a password-less probe fails silently and looks like "FE not ready" — always pass `-p` |
| T26 | `cdc_lag_seconds` shows values in the hundreds of thousands | not a bug: the drain replays raw topics from earliest, so historical records report their true age. Issue fresh source changes before curating if you want small numbers |

### 3.3 Known deferrals (not defects)

- **Least-privilege sink users** — the loaders currently connect as `root`/`admin`. A `dfs_sink` user
  (reserved: `nexus/dataflow-studio/{starrocks,clickhouse}-sink-password`) is production hardening.
- **Incremental fact loading** — the drain reloads the current fact snapshot; per-change fact CDC is
  a later enhancement. Dimensions are already properly incremental (SCD2).
- **Telemetry duplicates** — Kafka-engine ingestion is at-least-once and the telemetry tables are
  append-only, so a re-delivered batch can double-count. Accepted for telemetry (ADR-0008); the
  domain-data paths are all idempotent.
- **OTel export** — the sink records an OpenTelemetry counter, but nothing exports it until an OTLP
  endpoint is configured (`DFS_OTLP_ENDPOINT`). The observability tier lands in 3E.
