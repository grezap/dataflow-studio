# ADR-0005 — DbUp migrations for the StarRocks + ClickHouse sinks

- **Status:** accepted
- **Date:** 2026-07-12
- **Deciders:** Grigoris Zapantis

## Context

The two analytical sinks need schema migrations, and MASTER-PLAN enhancement E1 mandates **DbUp
(SQL-script, forward-only)** for StarRocks and ClickHouse — distinct from the reversible
**FluentMigrator** used for `OltpDb` (whose gate is `up → down → up`). Analytical stores are not
transactional and their DDL is effectively additive, so a forward-only, journal-tracked, idempotent
"apply → re-apply is a no-op" model fits; a rollback story does not.

Two wrinkles complicate a naive DbUp adoption:

1. **StarRocks** speaks the MySQL wire protocol, so `dbup-mysql` (which drives it via **MySqlConnector**,
   not Oracle's MySql.Data) *mostly* works — but DbUp's default journal table uses an
   `INT AUTO_INCREMENT` surrogate key, and StarRocks only allows `AUTO_INCREMENT` on PRIMARY KEY
   tables and always requires an explicit key model + `DISTRIBUTED BY` clause.
2. **ClickHouse** has **no DbUp provider at all**, lacks transactions, and creates cluster objects
   with `ON CLUSTER` DDL routed through Keeper — none of which DbUp's generic `ScriptExecutor` models.

A third, discovered during implementation: the **authored DDL** in
`nexus-platform-plan/schemas/dataflow-studio/README.md` was written on paper and never applied, so it
contains constructs real StarRocks/ClickHouse reject.

## Decision

**Split the two sinks; use DbUp for StarRocks, a DbUp-pattern runner for ClickHouse; parameterize
both so one script set validates on a throwaway container and applies to the lab cluster.**

### StarRocks — real DbUp + a StarRocks-compatible journal
`DataFlowStudio.Migrations.Starrocks` uses `dbup-mysql` with embedded `.sql` scripts and a custom
`StarRocksTableJournal : MySqlTableJournal` that overrides only `CreateSchemaTableSql` to emit a
PRIMARY KEY journal table (`PRIMARY KEY(scriptname) DISTRIBUTED BY HASH(scriptname)`). The inherited
`information_schema` lookups and INSERT/SELECT journal SQL work unchanged. `replication_num` is a DbUp
`$replicationNum$` variable (and a journal ctor arg) — **3** for the three-backend lab, **1** for a
single-backend container. The runner pre-creates the `dwh` + `analytics` databases (the journal lives
in `dwh`).

### ClickHouse — a purpose-built, DbUp-pattern runner
`DataFlowStudio.Migrations.Clickhouse` reimplements only the DbUp essentials over `ClickHouse.Client`:
a journal table of applied script names, ordered embedded scripts, `$variable$` substitution, and a
skip-if-applied loop. A `ClickHouseMigrationProfile` adapts the **same** scripts to the target: the
**lab** profile uses `Replicated*MergeTree` with the Guide-13 `/ch/tables/{shard}/…` Keeper paths +
`ON CLUSTER nexus_analytics`; the **single-node** profile uses plain `MergeTree` and no `ON CLUSTER`
(a lone container has no Keeper) but keeps the `nexus_analytics` cluster name so the `Distributed`
table still resolves against a one-node cluster the container defines.

### Idempotency gate (E1, forward-only variant)
`apply → re-apply` must leave the full schema present and execute **zero** scripts on the second run.
Each gate runs against a throwaway container — `starrocks/allin1-ubuntu:3.5.17` and
`clickhouse/clickhouse-server` — with a `DFS_{STARROCKS,CLICKHOUSE}_TEST_CONNECTION` env override to
target an external instance.

### Corrections to the authored canon DDL (StarRocks validity + cluster name)
The migration scripts are the source of truth; these fixes are mirrored back into the plan's
`schemas/dataflow-studio/README.md`:

1. **PK-model distribution key.** `dim_customer`/`dim_product`/`dim_warehouse` distributed by a
   *business* key (`customer_id`, …); StarRocks requires `DISTRIBUTED BY` to be a subset of the
   PRIMARY KEY, so they now distribute by their surrogate key.
2. **Colocation bucket count.** `fact_order_line` (32) and `fact_transaction` (16) shared
   `colocate_with "order_group"`; a colocation group requires identical bucket counts, so
   `fact_transaction` is aligned to 32.
3. **Key-column ordering.** `bridge_customer_seg` placed `weight` between its key columns; StarRocks
   requires key columns to be the first columns of the schema, so they are reordered to lead.
4. **Cluster name.** the ClickHouse DDL named `ON CLUSTER nexus_ch`; the lab cluster is
   `nexus_analytics` (Guide 13) — injected via the profile so it is never hard-coded in a script.

## Consequences

- **Positive:** one script set per sink, validated in CI on real containers and applied verbatim to
  the lab; the idempotency gate caught all four DDL defects before any lab write. Forward-only + a
  journal makes deploys and cold-rebuilds replay-safe.
- **Negative:** the ClickHouse runner is bespoke code to own (no upstream DbUp provider to lean on),
  and the lab-vs-container divergence lives in a profile that must be kept honest. Accepted: the
  divergence is small (engine + `ON CLUSTER` only) and every other line of DDL is shared.
- **Deferred:** live application to the lab StarRocks/ClickHouse happens when the sink workers load
  data (Sessions 3C/3D power those tiers on) — the schema is proven by the container gates first, so
  no lab tier need be running to land this migration slice.
