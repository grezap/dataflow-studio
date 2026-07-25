"""The ``dfs-conform`` entry point: parse args → build a typed config → run the Spark job."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import SparkSession

from dfs_conform.config import ConformConfig, SourceConfig, SourceKind
from dfs_conform.io import read_sources, write_mart
from dfs_conform.transform import build_customer_360


def _parse_args(argv: Sequence[str] | None) -> ConformConfig:
    parser = argparse.ArgumentParser(
        prog="dfs-conform",
        description="Conform the StarRocks Kimball star into a silver customer-360 Parquet mart.",
    )
    parser.add_argument("--source", choices=[k.value for k in SourceKind], default=SourceKind.PARQUET.value)
    parser.add_argument("--dim-customer-path", default="", help="Parquet path for dim_customer (source=parquet).")
    parser.add_argument("--fact-order-path", default="", help="Parquet path for fact_order (source=parquet).")
    parser.add_argument("--jdbc-url", default="", help="StarRocks JDBC URL (source=jdbc).")
    parser.add_argument("--jdbc-user", default="root")
    parser.add_argument("--jdbc-password", default="")
    parser.add_argument("--output", required=True, help="Output Parquet path for customer_360.")
    args = parser.parse_args(argv)

    return ConformConfig(
        source=SourceConfig(
            kind=SourceKind(args.source),
            dim_customer_path=args.dim_customer_path,
            fact_order_path=args.fact_order_path,
            jdbc_url=args.jdbc_url,
            jdbc_user=args.jdbc_user,
            jdbc_password=args.jdbc_password,
        ),
        output_path=args.output,
    )


def run(config: ConformConfig, spark: SparkSession) -> int:
    """Run the conformance job against an existing Spark session; returns the conformed row count."""
    dim_customer, fact_order = read_sources(spark, config.source)
    mart = build_customer_360(dim_customer, fact_order)
    write_mart(mart, config.output_path)
    return int(mart.count())


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    config = _parse_args(argv)
    spark = SparkSession.builder.appName(config.app_name).getOrCreate()
    try:
        rows = run(config, spark)
        print(f"dfs-conform: wrote {rows} customer_360 rows to {config.output_path}")
    finally:
        spark.stop()
    return 0
