-- dwh dimensions (Kimball) — reproduces schemas/dataflow-studio/README.md.
-- Every dimension is a StarRocks PRIMARY KEY table so the SCD2 loads (dim_customer, dim_product)
-- can upsert versions by surrogate key. replication_num = 3 matches the three backends (sr-be-1/2/3).
--
-- ADR-0005 fix: the authored DDL hash-distributed the PK dimensions by their *business* key
-- (customer_id / warehouse_id). StarRocks requires the DISTRIBUTED BY columns to be a subset of the
-- PRIMARY KEY, so these distribute by the surrogate key instead. The SCD2 grain is unchanged.

CREATE TABLE dwh.dim_date (
    date_key    INT      NOT NULL,
    full_date   DATE     NOT NULL,
    year        SMALLINT NULL,
    quarter     TINYINT  NULL,
    month       TINYINT  NULL,
    day         TINYINT  NULL,
    day_of_week TINYINT  NULL,
    is_weekend  BOOLEAN  NULL,
    iso_week    SMALLINT NULL
)
PRIMARY KEY(date_key)
DISTRIBUTED BY HASH(date_key) BUCKETS 1
PROPERTIES ("replication_num" = "$replicationNum$");

CREATE TABLE dwh.dim_customer (
    customer_sk        BIGINT       NOT NULL,
    customer_id        BIGINT       NOT NULL,
    customer_code      VARCHAR(32)  NULL,
    display_name       VARCHAR(200) NULL,
    email              VARCHAR(256) NULL,
    preferred_locale   VARCHAR(10)  NULL,
    status             TINYINT      NULL,
    lifetime_value_usd DECIMAL(18,2) NULL,
    valid_from         DATETIME     NOT NULL,
    valid_to           DATETIME     NOT NULL,
    is_current         BOOLEAN      NOT NULL
)
PRIMARY KEY(customer_sk)
DISTRIBUTED BY HASH(customer_sk) BUCKETS 12
PROPERTIES ("replication_num" = "$replicationNum$");

CREATE TABLE dwh.dim_product (
    product_sk     BIGINT       NOT NULL,
    product_id     BIGINT       NOT NULL,
    sku            VARCHAR(64)  NULL,
    category_id    INT          NULL,
    category_name  VARCHAR(200) NULL,
    display_name   VARCHAR(300) NULL,
    list_price_usd DECIMAL(18,4) NULL,
    valid_from     DATETIME     NOT NULL,
    valid_to       DATETIME     NOT NULL,
    is_current     BOOLEAN      NOT NULL
)
PRIMARY KEY(product_sk)
DISTRIBUTED BY HASH(product_sk) BUCKETS 12
PROPERTIES ("replication_num" = "$replicationNum$");

CREATE TABLE dwh.dim_warehouse (
    warehouse_sk  INT          NOT NULL,
    warehouse_id  INT          NOT NULL,
    code          VARCHAR(16)  NULL,
    name          VARCHAR(200) NULL,
    region        VARCHAR(100) NULL,
    country_iso2  CHAR(2)      NULL,
    timezone_iana VARCHAR(64)  NULL
)
PRIMARY KEY(warehouse_sk)
DISTRIBUTED BY HASH(warehouse_sk) BUCKETS 1
PROPERTIES ("replication_num" = "$replicationNum$");

CREATE TABLE dwh.dim_carrier (
    carrier_sk    INT         NOT NULL,
    carrier       VARCHAR(32) NOT NULL,
    service_level VARCHAR(32) NULL
)
PRIMARY KEY(carrier_sk)
DISTRIBUTED BY HASH(carrier_sk) BUCKETS 1
PROPERTIES ("replication_num" = "$replicationNum$");
