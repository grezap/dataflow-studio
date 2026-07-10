# SQL Showcase — DataFlow Studio

Advanced SQL techniques exercised by this project (MASTER-PLAN §2 + E28). The portfolio-wide
catalog in `portfolio-index/docs/sql-depth.md` cross-references these.

> **Acceptance-gate status:** ≥3 documented artifacts required (MASTER-PLAN §6). Three land with the
> Week-1 OltpDb schema below; window functions, recursive CTEs, `MERGE ... OUTPUT`, and `FOR JSON`
> arrive with the Commerce write-side and the StarRocks/ClickHouse loaders (Weeks 2–3).

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

## Planned (Weeks 2–3)

- Recursive CTE over `dbo.ProductCategories` (self-referencing hierarchy).
- Window functions with frames for running order totals / customer lifetime value.
- `MERGE ... OUTPUT` for SCD2 dimension upserts into StarRocks `dwh`.
- `FOR JSON PATH` to shape CDC payloads; ClickHouse `AggregatingMergeTree` MV (`pipeline_latency_by_hour`).
