# ADR-0007 — A data-driven curation catalog (and the Ingestion module's non-AOT stance)

- **Status:** accepted
- **Date:** 2026-07-17
- **Deciders:** Grigoris Zapantis

## Context

Week 2 proved the CDC → curated-Avro pattern for a single entity (customers), with the reshaping
logic inlined in the `dfs trace` demo tool. Week 3 needs curated events for **all ten order-flow
entities** (customers, product categories, products, warehouses, customer addresses, orders, order
lines, transactions, shipments, product inventory) so the StarRocks dimensions/facts have sources.

Two forces shape the design:

1. **Ten near-identical workers would be ten places to drift.** Each entity's curation is "read the
   Debezium `after` image, project a handful of typed fields, produce a curated Avro record" — the
   same algorithm with different field lists.
2. **The E4/AOT tension.** ADR-0002 framed the Ingestion module as a Native-AOT Kafka worker, but
   ADR-0004 chose Debezium (not a .NET worker) for log-based capture, and ADR-0003 established that
   the Confluent Avro serdes are reflection-based and therefore **not** Native-AOT. There is no
   genuine AOT .NET Kafka worker left in the chosen architecture.

## Decision

**Curation is data, not code, and the Ingestion module is the (non-AOT) curation worker.**

- **A curation catalog.** Each entity is one `EntityCurationSpec` — raw topic, curated topic, record
  name, key field, and a list of `CuratedField`s (curated name ← source column, with a
  `CuratedFieldKind`). The spec builds its own curated Avro schema (business fields + a common
  `operation`/`sourceTsMs`/`curatedAtUtc` envelope). Adding an entity is a new list entry.
- **A single engine + projector.** `CuratedRecordProjector` is pure (Debezium `after` → Avro
  `GenericRecord`), so it is exhaustively unit-testable with no Kafka. `CurationEngine` subscribes to
  every raw topic, dispatches each message to its spec, and re-produces curated Avro. It runs
  continuously (the hosted `CurationWorker`) or in drain mode (the runnable `dfs curate` console and
  the live source-replay) — one code path.
- **Field encodings pinned to the connector.** Decimals are carried as strings
  (`decimal.handling.mode=string`) and temporal columns as epoch-millisecond longs
  (`time.precision.mode=connect`), so the curated types are stable and source-independent.
- **The Ingestion module drops its AOT badge.** It gains `Nexus.Kafka`/`Nexus.Avro` + Confluent and
  is deliberately non-AOT (ADR-0003/0004 made an AOT worker impossible here). The one E4 invariant
  that survives — **no EF Core** — still holds and is still enforced by the architecture tests. The
  AOT showcase lives on in the migration tools and the analyzer-clean libraries.

## Consequences

- **Positive:** ten entities, one worker; the projection is unit-tested in isolation; the same engine
  serves the production worker, the demo, and the live replay. The wire contracts are generated from
  the catalog and documented in AsyncAPI.
- **Negative:** the catalog's field kinds are coupled to the Debezium connector settings — a
  connector reconfigured to a different decimal/temporal mode would need the projector updated. This
  is called out in the connector config and the catalog docs.
- **Supersedes** the ADR-0002 characterization of the Ingestion module as Native-AOT; the "no EF
  Core on the CDC path" rule is retained.
