# ADR-0014 — A PySpark silver-layer conformance job (the Python dimension)

- **Status:** accepted
- **Date:** 2026-07-25
- **Deciders:** Grigoris Zapantis

## Context

The portfolio's four skill dimensions (MASTER-PLAN §2) include **Python**, and the grid lists
DataFlow Studio among the Python-applicable projects. The §6 acceptance gate requires "≥1 PySpark job OR
ML training script". Weeks 1–3 built the pipeline entirely in .NET (CDC → curated Avro → StarRocks DWH +
ClickHouse telemetry); there was no Python leg yet.

The question was **what** the Python job should do so it is real, not a token. The pipeline already
produces a **gold** Kimball star. A natural, honest next layer is a **silver conformance** step: reshape
the gold star into an analyst-facing mart. That is exactly the kind of work PySpark is used for in a
lakehouse, and it reads the artifact the .NET sink produces — so it fits the pipeline rather than bolting
on an unrelated demo.

## Decision

**Add `pyspark/` — a small, strictly-typed PySpark job (`dfs-conform`) that conforms the StarRocks Kimball
star into a `customer_360` silver mart.**

- **The transform is a pure function of DataFrames** (`build_customer_360(dim_customer, fact_order)`):
  filter `dim_customer` to the SCD2 *current* version, left-join the `fact_order` aggregates
  (order-count, total + average revenue, first/last order date), zero-fill customers with no orders. Being
  pure and I/O-free, it is **unit-tested on a local `SparkSession`** — the lab is not required to prove it.
- **I/O is separated** (`io.py`): read the two tables from Parquet (offline / tests) or the live StarRocks
  FE over the MySQL wire via JDBC; write the mart as Parquet. Config is **Pydantic v2** (`config.py`),
  validated (parquet needs paths; jdbc needs a URL).
- **Modern toolchain, enforced** (E27): uv for the environment, Ruff for lint, **mypy `--strict`**, Pydantic
  v2. A CI job runs ruff + mypy + the pytest suite (with a JVM via `setup-java`), so the Spark logic is
  verified reproducibly even though the dev host here has no JVM.

### Alternatives considered

| Option | Why not |
|---|---|
| An ML training script (the gate's "OR") | The pipeline has no natural ML target yet; a contrived model would be a token, not a showcase. Conformance is genuinely part of this data pipeline. |
| A pandas / Polars job | Simpler, but the grid + gate specifically call out **PySpark**, and Spark is the honest tool for reading a warehouse table into a distributed mart. Polars is reserved for the lighter analytics tooling (E27). |
| Read ClickHouse instead of StarRocks | ClickHouse holds *telemetry*, not the domain star. The customer-360 mart belongs on the gold `dwh` star. |

## Consequences

- **Positive:** the Python dimension is satisfied by real, tested, strictly-typed code that consumes the
  pipeline's own output — a silver layer above the gold star. The pure-transform design keeps it honest
  (unit-tested, no lab). Closes §6 box #4.
- **Negative:** it adds a second toolchain (Python + a JVM) to the repo; the Spark test needs Java, so it
  runs in a dedicated CI job rather than the .NET build. The JDBC path needs the MySQL driver on the Spark
  classpath (documented) — it is not exercised by the offline tests.
- **Verification:** ruff + mypy `--strict` pass locally; the transform's SCD2-current filter + zero-fill
  are covered by pytest on a local SparkSession (green in CI with Java). Canon: satisfies MASTER-PLAN §6 #4
  and the **Python** dimension (§2).
