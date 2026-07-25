"""The pure conformance transform — a function of DataFrames, so it unit-tests without I/O.

``build_customer_360`` conforms the gold Kimball star into a silver customer-360 mart: one row per
*current* customer (the SCD2 current version) enriched with their order aggregates. A left join keeps
customers with no orders (zero-filled), so the mart is a complete customer dimension, not just buyers.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_customer_360(dim_customer: DataFrame, fact_order: DataFrame) -> DataFrame:
    """Conform ``dim_customer`` (SCD2) + ``fact_order`` into the ``customer_360`` mart.

    Args:
        dim_customer: the ``dwh.dim_customer`` SCD2 dimension (all versions; ``is_current`` flags the live one).
        fact_order: the ``dwh.fact_order`` fact (one row per order, keyed by ``customer_sk``).

    Returns:
        One row per current customer with order-count / revenue / recency aggregates, zero-filled for
        customers with no orders.
    """
    current = dim_customer.filter(F.col("is_current") == 1)

    order_aggs = fact_order.groupBy("customer_sk").agg(
        F.count("order_id").alias("order_count"),
        F.round(F.sum("total_usd"), 2).alias("total_revenue_usd"),
        F.round(F.avg("total_usd"), 2).alias("avg_order_value_usd"),
        F.min("order_date_key").alias("first_order_date_key"),
        F.max("order_date_key").alias("last_order_date_key"),
    )

    conformed = current.join(order_aggs, on="customer_sk", how="left").select(
        F.col("customer_sk"),
        F.col("customer_id"),
        F.col("display_name"),
        F.col("email"),
        F.col("status"),
        F.coalesce(F.col("order_count"), F.lit(0)).alias("order_count"),
        F.coalesce(F.col("total_revenue_usd"), F.lit(0.0)).alias("total_revenue_usd"),
        F.coalesce(F.col("avg_order_value_usd"), F.lit(0.0)).alias("avg_order_value_usd"),
        F.col("first_order_date_key"),
        F.col("last_order_date_key"),
    )

    return conformed.orderBy("customer_sk")
