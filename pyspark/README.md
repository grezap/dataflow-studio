# dfs-conform — silver-layer conformance (PySpark)

The Python leg of DataFlow Studio. The .NET warehouse sink builds the **gold** Kimball star in StarRocks
(`dwh.dim_customer` SCD2 + `dwh.fact_order`); this PySpark job reads that star and conforms it into a
**silver** `customer_360` mart — one row per *current* customer, enriched with order-count, revenue, and
recency aggregates (customers with no orders are kept and zero-filled).

It is deliberately small and **testable**: the transform ([`transform.py`](src/dfs_conform/transform.py))
is a pure function of DataFrames, unit-tested on a local `SparkSession`, so no lab is needed to prove it.

## Toolchain

Modern Python, enforced (MASTER-PLAN E27): **[uv](https://docs.astral.sh/uv/)** for envs +
**[Ruff](https://docs.astral.sh/ruff/)** lint + **[mypy](https://mypy.readthedocs.io/) `--strict`** +
**[Pydantic v2](https://docs.pydantic.dev/)** for typed config. Requires Python ≥3.11 and a JVM (Spark).

```bash
uv sync                       # create the env from pyproject.toml (dev group included)
uv run ruff check src tests   # lint
uv run mypy                    # strict type-check
uv run pytest                  # the transform tests (local SparkSession — needs Java 17+)
```

## Run it

```bash
# Offline (Parquet exports of the two star tables — what the tests use):
uv run dfs-conform \
  --source parquet \
  --dim-customer-path ./_in/dim_customer.parquet \
  --fact-order-path   ./_in/fact_order.parquet \
  --output            ./_out/customer_360.parquet

# Live (StarRocks FE over the MySQL wire :9030 — needs the MySQL JDBC driver on the Spark classpath):
spark-submit --packages com.mysql:mysql-connector-j:8.4.0 \
  -m dfs_conform.cli \
  --source jdbc --jdbc-url 'jdbc:mysql://192.168.70.31:9030/dwh' --jdbc-user root --jdbc-password "$SR_PW" \
  --output ./_out/customer_360.parquet
```

## Layout

```
src/dfs_conform/
  config.py     Pydantic v2 config (SourceKind parquet|jdbc, ConformConfig)
  transform.py  build_customer_360(dim_customer, fact_order) -> DataFrame   (the pure, tested core)
  io.py         read the star (parquet|jdbc) + write the mart (parquet)
  cli.py        the `dfs-conform` entry point
tests/          transform tests on a local SparkSession
```

See [ADR-0014](../docs/adr/ADR-0014-pyspark-silver-conformance.md) for the design rationale.
