# SQL Showcase — DataFlow Studio

Advanced SQL techniques exercised by this project (MASTER-PLAN §2 + E28). The portfolio-wide
catalog in `portfolio-index/docs/sql-depth.md` cross-references these.

> **Acceptance-gate status:** ≥3 documented artifacts required (MASTER-PLAN §6) — **met: 9.**
> §1–§4 are the Week-1/2 OltpDb + CDC surface; §5–§8 are live against the loaded StarRocks star
> (Week 3); §9's ClickHouse MV is migrated and fills with the Week-3D telemetry sink.

## 1. System-versioned temporal tables

`dbo.Customers` and `dbo.Products` are system-versioned. SQL Server transparently maintains a full
history table, so point-in-time queries need no application bookkeeping — and the temporal history
feeds SCD2 dimension loads downstream in StarRocks.

```sql
-- All versions of a customer row that were current during Q1 2026.
SELECT CustomerId, DisplayName, LifetimeValueUsd, ValidFrom, ValidTo
FROM dbo.Customers
FOR SYSTEM_TIME BETWEEN '2026-01-01' AND '2026-03-31'
WHERE CustomerId = @customerId
ORDER BY ValidFrom;
```

Migration note: the `Down()` must `SET (SYSTEM_VERSIONING = OFF)` before dropping the table and its
history table — proven by the E1 up → down → up gate.

## 2. Persisted computed column

`dbo.OrderLines.LineTotalUsd` is a `PERSISTED` computed column — the arithmetic is stored and
indexable, so line totals never drift from `Quantity`, `UnitPriceUsd`, and `DiscountUsd`.

```sql
LineTotalUsd AS (CAST(Quantity * UnitPriceUsd - DiscountUsd AS DECIMAL(18,2))) PERSISTED
```

## 3. `ROWVERSION` optimistic concurrency

Every business table carries a `ROWVERSION` (`row_version`) — a database-assigned, monotonically
increasing 8-byte token. The Commerce write-side uses it for lost-update detection without holding
locks:

```sql
UPDATE dbo.Customers
SET LifetimeValueUsd = @newValue, modified_utc = SYSUTCDATETIME(), modified_by = @actor
WHERE CustomerId = @id AND row_version = @expectedRowVersion;   -- 0 rows affected => concurrency conflict
```

## 4. Change Data Capture (CDC)

CDC is enabled on `OltpDb` so the pipeline reads committed changes from the transaction log — never
by querying (and contending with) the live OLTP tables. Enablement:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'Customers',
     @role_name = NULL, @supports_net_changes = 0;   -- (repeated per business table)
```

Read what CDC captured (this is exactly what Debezium streams):

```sql
SELECT __$operation,                       -- 1=delete 2=insert 3/4=update(before/after)
       sys.fn_varbintohexstr(__$start_lsn) AS lsn,
       CustomerCode, DisplayName
FROM OltpDb.cdc.dbo_Customers_CT
ORDER BY __$start_lsn DESC;
```

The SQL Agent capture job populates `cdc.dbo_*_CT` from the log; CDC on the system-versioned temporal
tables (`Customers`, `Products`) is supported and works alongside `SYSTEM_VERSIONING`.

## 5. Recursive CTE — the category hierarchy (SQL Server)

`dbo.ProductCategories` self-references via `ParentId`. A recursive CTE walks it and materializes the
breadcrumb path and depth in one pass:

```sql
WITH category_tree AS (
    SELECT CategoryId, ParentId, Name, Slug,
           CAST(Name AS NVARCHAR(1000)) AS Path, 0 AS Depth
    FROM dbo.ProductCategories
    WHERE ParentId IS NULL                                  -- anchor: the roots
    UNION ALL
    SELECT c.CategoryId, c.ParentId, c.Name, c.Slug,
           CAST(t.Path + N' > ' + c.Name AS NVARCHAR(1000)), t.Depth + 1
    FROM dbo.ProductCategories c
    JOIN category_tree t ON c.ParentId = t.CategoryId       -- recursive member
)
SELECT Depth, Path, Slug FROM category_tree ORDER BY Path;
```

Against the seeded data: `Electronics > Audio` and `Home & Kitchen > Kitchen` at depth 1.

## 6. Window functions with frames — running totals + share of lifetime (StarRocks)

Run against the loaded star. A running total needs an explicit frame; the partition-wide `SUM` in the
same SELECT gives each order's share of that customer's lifetime spend:

```sql
SELECT c.display_name, o.order_id, d.full_date, o.total_usd,
       SUM(o.total_usd) OVER (PARTITION BY o.customer_sk ORDER BY d.full_date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       ROUND(100.0 * o.total_usd / SUM(o.total_usd) OVER (PARTITION BY o.customer_sk), 1) AS pct_of_lifetime,
       RANK() OVER (ORDER BY o.total_usd DESC) AS value_rank
FROM dwh.fact_order o
JOIN dwh.dim_customer c ON o.customer_sk = c.customer_sk AND c.is_current = 1
JOIN dwh.dim_date     d ON o.order_date_key = d.date_key
ORDER BY c.display_name, d.full_date;
```

## 7. SCD2 dimension load — close-current + insert-version (StarRocks PRIMARY KEY model)

**StarRocks has no `MERGE` statement**, so the loader uses what the PRIMARY KEY model does give:
row-level `UPDATE` to close a version, and an upserting batched `INSERT` for the new one. The
attribute comparison is what makes it idempotent — an unchanged record inserts nothing.

```sql
-- 1. what is current for this business key?
SELECT customer_sk, customer_code, display_name, email, preferred_locale,
       status, CAST(lifetime_value_usd AS CHAR)
FROM dwh.dim_customer
WHERE customer_id = 42 AND is_current = 1;

-- 2. attributes changed -> close the current version (PK tables support UPDATE)
UPDATE dwh.dim_customer
SET is_current = 0, valid_to = '2026-07-17 12:00:00'
WHERE customer_sk IN (7);

-- 3. insert the new version. One multi-row INSERT per load: StarRocks creates a version per
--    INSERT, so row-by-row loading causes a version explosion.
INSERT INTO dwh.dim_customer
    (customer_sk, customer_id, customer_code, display_name, email, preferred_locale,
     status, lifetime_value_usd, valid_from, valid_to, is_current)
VALUES (12, 42, 'SEED-C001', 'Ada Lovelace (Countess)', 'ada@example.com', 'en-US', 1, 318.18,
        '2026-07-17 12:00:00', '9999-12-31 00:00:00', 1);
```

Point-in-time queries then fall out of the interval: `WHERE '<t>' >= valid_from AND '<t>' < valid_to`.

## 8. The Kimball star join

The payoff — a fact resolved through its dimensions, with SCD2 versions filtered to the current one:

```sql
SELECT c.display_name, p.display_name AS product, w.code AS warehouse,
       l.quantity, l.line_total_usd, d.full_date
FROM dwh.fact_order_line l
JOIN dwh.dim_customer  c ON l.customer_sk  = c.customer_sk  AND c.is_current = 1
JOIN dwh.dim_product   p ON l.product_sk   = p.product_sk   AND p.is_current = 1
JOIN dwh.dim_warehouse w ON l.warehouse_sk = w.warehouse_sk
JOIN dwh.dim_date      d ON l.order_date_key = d.date_key
ORDER BY l.order_line_id;
```

## 9. Aggregate states — `AggregatingMergeTree` (ClickHouse)

`analytics.pipeline_latency_by_hour` stores **partial aggregate states**, not finished numbers — the
MV writes `quantilesState`/`countState` on ingest, and readers finalize with the `-Merge` combinators.
That is what makes hourly percentiles cheap over a high-volume stream:

```sql
SELECT pipeline, stage, hour,
       quantilesMerge(0.5, 0.95, 0.99)(p_state) AS p50_p95_p99,
       countMerge(events_state)                 AS events
FROM analytics.pipeline_latency_by_hour
GROUP BY pipeline, stage, hour
ORDER BY hour DESC;
```

The table + MV are migrated (ADR-0005); the telemetry that fills them lands with the Week-3D sink.
