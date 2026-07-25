"""Typed configuration for the conformance job (Pydantic v2)."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceKind(StrEnum):
    """Where the Kimball star is read from."""

    PARQUET = "parquet"
    """Parquet exports of ``dwh.dim_customer`` + ``dwh.fact_order`` (used by tests + offline runs)."""

    JDBC = "jdbc"
    """The live StarRocks FE over the MySQL wire (:9030)."""


class SourceConfig(BaseModel):
    """How to read the two source tables."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: SourceKind = SourceKind.PARQUET
    dim_customer_path: str = Field(default="", description="Parquet path for dim_customer (kind=parquet).")
    fact_order_path: str = Field(default="", description="Parquet path for fact_order (kind=parquet).")
    jdbc_url: str = Field(default="", description="StarRocks JDBC URL, e.g. jdbc:mysql://192.168.70.31:9030/dwh.")
    jdbc_user: str = Field(default="root")
    jdbc_password: str = Field(default="")

    @model_validator(mode="after")
    def _check(self) -> SourceConfig:
        if self.kind is SourceKind.PARQUET and not (self.dim_customer_path and self.fact_order_path):
            raise ValueError("parquet source requires dim_customer_path and fact_order_path")
        if self.kind is SourceKind.JDBC and not self.jdbc_url:
            raise ValueError("jdbc source requires jdbc_url")
        return self


class ConformConfig(BaseModel):
    """The full job configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: SourceConfig
    output_path: str = Field(description="Where to write the conformed customer_360 Parquet.")
    app_name: str = Field(default="dfs-conform")
