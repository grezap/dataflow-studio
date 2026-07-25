"""Reading the star + writing the mart. Kept apart from the transform so the transform stays pure."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from dfs_conform.config import SourceConfig, SourceKind


def read_sources(spark: SparkSession, source: SourceConfig) -> tuple[DataFrame, DataFrame]:
    """Read ``(dim_customer, fact_order)`` from Parquet (offline/tests) or the live StarRocks JDBC."""
    if source.kind is SourceKind.PARQUET:
        return (
            spark.read.parquet(source.dim_customer_path),
            spark.read.parquet(source.fact_order_path),
        )

    # StarRocks speaks the MySQL wire on :9030; needs the MySQL JDBC driver on the Spark classpath
    # (--packages com.mysql:mysql-connector-j:8.4.0). SslMode=disabled — the FE query port is TLS-off.
    reader = (
        spark.read.format("jdbc")
        .option("url", source.jdbc_url)
        .option("user", source.jdbc_user)
        .option("password", source.jdbc_password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
    )
    return (
        reader.option("dbtable", "dwh.dim_customer").load(),
        reader.option("dbtable", "dwh.fact_order").load(),
    )


def write_mart(mart: DataFrame, output_path: str) -> None:
    """Write the conformed mart as Parquet (overwrite — the mart is a full rebuild each run)."""
    mart.write.mode("overwrite").parquet(output_path)
