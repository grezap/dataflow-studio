# ADR-0002 — Dapper + FluentMigrator on AOT paths; no EF Core on the Kafka worker

- **Status:** accepted
- **Date:** 2026-07-11
- **Deciders:** Grigoris Zapantis

## Context

MASTER-PLAN enhancement **E4** resolves the "AOT + EF Core tension": the CDC → Kafka worker paths
are intended for **Native AOT** publish, and EF Core's model-building relies on reflection and
runtime code generation that fight trimming/AOT. E1 further mandates **FluentMigrator** (with
explicit `Up()` + `Down()`) as the migration tool for SQL Server, with a CI gate proving
`up → down → up` on a fresh container.

## Decision

- The **Ingestion** module (the CDC → Kafka worker) is a Native-AOT path. It uses **Dapper** for
  CDC reads and is marked `IsAotCompatible` with the trim analyzer on. **EF Core is banned** here.
- **`OltpDb`** schema is owned by **FluentMigrator** migrations (`DataFlowStudio.Migrations.Oltp`),
  each with a real reversible `Down()`. The migration runner is a deploy-time tool (reflection-based
  discovery), **not** an AOT path — but it still uses no EF Core (FluentMigrator + raw SQL).
- Both bans are enforced by a **NetArchTest** rule: neither the Ingestion assembly nor the
  Migrations assembly may depend on `Microsoft.EntityFrameworkCore`.
- EF Core remains *permitted* on non-AOT paths (e.g. a future read-model), per E4 — but the whole
  solution stays Dapper-first for driver consistency.

The authored temporal DDL (system-versioned `Customers`/`Products`), the persisted computed column
(`OrderLines.LineTotalUsd`), and `ROWVERSION` concurrency tokens are expressed as raw SQL via
`Execute.Sql(...)` in the migrations for exact fidelity to `schemas/dataflow-studio`.

## Consequences

- **Positive:** the Kafka worker can be published as a small, fast, dependency-light AOT binary;
  the "no EF on AOT" rule can never silently regress because a test guards it.
- **Positive:** `Down()` on every migration makes the E1 up → down → up gate meaningful and proves
  the schema is fully reversible, including tearing down system-versioning before dropping temporal
  tables.
- **Negative:** hand-written SQL DDL is more verbose than a fluent-API model and must be kept in
  sync with `schemas/dataflow-studio` by hand — accepted, since the schema is canon and rarely churns.
