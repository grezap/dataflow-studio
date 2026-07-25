"""Unit tests for the conformance transform, on a local SparkSession."""

from __future__ import annotations

from pyspark.sql import DataFrame, Row, SparkSession

from dfs_conform.transform import build_customer_360


def _dim_customer(spark: SparkSession) -> DataFrame:
    # customer_sk 1 = Ada (current); sk 2 = an OLD version of Bob (is_current=0); sk 3 = Bob current;
    # sk 4 = Cleo current with no orders. The old version must be excluded from the mart.
    return spark.createDataFrame(
        [
            Row(customer_sk=1, customer_id=100, display_name="Ada", email="ada@x.io", status=1, is_current=1),
            Row(customer_sk=2, customer_id=200, display_name="Bob0", email="old@x.io", status=1, is_current=0),
            Row(customer_sk=3, customer_id=200, display_name="Bob", email="bob@x.io", status=1, is_current=1),
            Row(customer_sk=4, customer_id=300, display_name="Cleo", email="cleo@x.io", status=1, is_current=1),
        ]
    )


def _fact_order(spark: SparkSession) -> DataFrame:
    # Ada (sk 1): two orders totalling 150.00; Bob current (sk 3): one order of 40.00; Cleo: none.
    return spark.createDataFrame(
        [
            Row(order_id=10, customer_sk=1, order_date_key=20260701, total_usd=100.00),
            Row(order_id=11, customer_sk=1, order_date_key=20260705, total_usd=50.00),
            Row(order_id=12, customer_sk=3, order_date_key=20260702, total_usd=40.00),
        ]
    )


def test_customer_360_aggregates_current_customers_only(spark: SparkSession) -> None:
    mart = build_customer_360(_dim_customer(spark), _fact_order(spark))
    rows = {r["customer_sk"]: r for r in mart.collect()}

    # The old SCD2 version (sk 2) is excluded; only current customers remain.
    assert set(rows.keys()) == {1, 3, 4}

    ada = rows[1]
    assert ada["order_count"] == 2
    assert ada["total_revenue_usd"] == 150.00
    assert ada["avg_order_value_usd"] == 75.00
    assert ada["first_order_date_key"] == 20260701
    assert ada["last_order_date_key"] == 20260705


def test_customer_with_no_orders_is_zero_filled(spark: SparkSession) -> None:
    mart = build_customer_360(_dim_customer(spark), _fact_order(spark))
    cleo = next(r for r in mart.collect() if r["customer_sk"] == 4)

    assert cleo["order_count"] == 0
    assert cleo["total_revenue_usd"] == 0.0
    assert cleo["first_order_date_key"] is None
