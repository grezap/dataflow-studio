# ADR-0001 — Modular Monolith with enforced module boundaries

- **Status:** accepted
- **Date:** 2026-07-11
- **Deciders:** Grigoris Zapantis

## Context

DataFlow Studio (Portfolio Project #1) moves a commerce dataset through a pipeline: SQL Server AG
(`OltpDb`, source of truth) → CDC → Kafka (Avro) → StarRocks Kimball DWH + ClickHouse telemetry.
The MASTER-PLAN grid assigns this project the **Modular Monolith** architecture, and every project
must demonstrably exercise the ".NET engineering + architecture" skill dimension with boundaries
enforced by tests (MASTER-PLAN §2).

A single deployable keeps the inner-dev loop and operational surface small (one host, one Aspire
AppHost) while still teaching real boundary discipline — the pipeline's stages are genuinely
separable concerns (write-side OLTP, CDC ingestion, warehouse loading, telemetry).

## Decision

Structure the solution as a **modular monolith**:

- One shared **`SharedKernel`** assembly holds cross-cutting primitives (Result pattern, audit
  columns, the `IModule` contract, and `IntegrationEvent` — the only cross-module coupling surface).
- Each pipeline concern is an isolated **module** assembly: `Commerce` (OLTP write-side),
  `Ingestion` (CDC → Kafka worker), `Warehouse` (StarRocks DWH), `Telemetry` (ClickHouse).
- Modules **never reference one another**. Cross-module communication flows through SharedKernel
  contracts (integration events); the Ingestion worker operates on the `IntegrationEvent`
  abstraction plus a mapper registry populated by the composition root, not on concrete module types.
- The **`Api`** host is the single composition root — the only project allowed to reference every
  module. It instantiates each module explicitly (no reflection) and lets it self-register.

These rules are enforced by **NetArchTest** architecture tests that fail the build if a module
takes a dependency on another module or on the host.

## Consequences

- **Positive:** boundaries are executable, not aspirational; a later extraction to microservices
  (Project #11) is mechanical because modules already have clean seams; one deployable keeps the
  demo/operational surface minimal.
- **Positive:** the explicit (reflection-free) module wiring keeps the host trim/AOT-friendly.
- **Negative:** cross-module contracts must be deliberately placed in SharedKernel, which is slight
  friction versus a direct reference — accepted, because it is exactly the discipline being showcased.
- **Follow-up:** when a *second* consumer needs a SharedKernel primitive, it is extracted to the
  `nexus-shared` NuGet family per E8 (MASTER-PLAN §7.4), not before.
