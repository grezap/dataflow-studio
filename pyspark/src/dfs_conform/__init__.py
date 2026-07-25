"""DataFlow Studio silver-layer conformance (PySpark).

Reads the StarRocks Kimball star (dim_customer + fact_order) produced by the .NET warehouse sink and
builds a conformed ``customer_360`` mart — the silver layer above the gold star. The transform is a pure
function of DataFrames so it is unit-testable on a local ``SparkSession`` without the lab.
"""

from dfs_conform.config import ConformConfig, SourceKind
from dfs_conform.transform import build_customer_360

__all__ = ["ConformConfig", "SourceKind", "build_customer_360"]
