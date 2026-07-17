-- dwh facts + the customer-segment bridge (Kimball) — reproduces schemas/dataflow-studio/README.md.
-- Facts are DUPLICATE KEY tables, range-partitioned by their date key (partitions are added by the
-- loader as data arrives), hash-distributed by order_id so order-grain facts colocate for joins.
--
-- ADR-0005 fix: the authored DDL colocated fact_order_line (BUCKETS 32) and fact_transaction
-- (BUCKETS 16) in the same "order_group". A StarRocks colocation group requires an identical bucket
-- count across its tables, so fact_transaction is aligned to BUCKETS 32.

CREATE TABLE dwh.fact_order (
    order_id            BIGINT NOT NULL,
    order_date_key      INT    NOT NULL,
    customer_sk         BIGINT NOT NULL,
    billing_address_id  BIGINT NULL,
    shipping_address_id BIGINT NULL,
    status              TINYINT NULL,
    subtotal_usd        DECIMAL(18,2) NULL,
    tax_usd             DECIMAL(18,2) NULL,
    shipping_usd        DECIMAL(18,2) NULL,
    total_usd           DECIMAL(18,2) NULL,
    currency            CHAR(3) NULL,
    placed_at_utc       DATETIME NULL
)
DUPLICATE KEY(order_id, order_date_key)
PARTITION BY RANGE(order_date_key) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES ("replication_num" = "$replicationNum$");

CREATE TABLE dwh.fact_order_line (
    order_line_id  BIGINT NOT NULL,
    order_id       BIGINT NOT NULL,
    order_date_key INT    NOT NULL,
    customer_sk    BIGINT NULL,
    product_sk     BIGINT NULL,
    warehouse_sk   INT    NULL,
    quantity       INT    NULL,
    unit_price_usd DECIMAL(18,4) NULL,
    discount_usd   DECIMAL(18,4) NULL,
    line_total_usd DECIMAL(18,2) NULL
)
DUPLICATE KEY(order_line_id)
PARTITION BY RANGE(order_date_key) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES ("replication_num" = "$replicationNum$", "colocate_with" = "order_group");

CREATE TABLE dwh.fact_transaction (
    transaction_id BIGINT NOT NULL,
    order_id       BIGINT NOT NULL,
    txn_date_key   INT    NOT NULL,
    provider       VARCHAR(32) NULL,
    kind           TINYINT NULL,
    amount_usd     DECIMAL(18,2) NULL,
    status         TINYINT NULL,
    occurred_at_utc DATETIME NULL
)
DUPLICATE KEY(transaction_id)
PARTITION BY RANGE(txn_date_key) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES ("replication_num" = "$replicationNum$", "colocate_with" = "order_group");

CREATE TABLE dwh.fact_inventory_snap (
    snap_date_key INT    NOT NULL,
    product_sk    BIGINT NOT NULL,
    warehouse_sk  INT    NOT NULL,
    on_hand       INT    NULL,
    reserved      INT    NULL,
    reorder_point INT    NULL,
    safety_stock  INT    NULL
)
DUPLICATE KEY(snap_date_key, product_sk, warehouse_sk)
DISTRIBUTED BY HASH(product_sk) BUCKETS 16
PROPERTIES ("replication_num" = "$replicationNum$");

-- ADR-0005 fix: StarRocks key-model tables require the key columns to be the first columns of the
-- schema, in order. The authored DDL placed `weight` between the key columns, so the three key
-- columns are reordered to lead (the grain and semantics are unchanged).
CREATE TABLE dwh.bridge_customer_seg (
    customer_sk    BIGINT      NOT NULL,
    segment_code   VARCHAR(32) NOT NULL,
    as_of_date_key INT         NOT NULL,
    weight         DECIMAL(9,6) NOT NULL
)
DUPLICATE KEY(customer_sk, segment_code, as_of_date_key)
DISTRIBUTED BY HASH(customer_sk) BUCKETS 12
PROPERTIES ("replication_num" = "$replicationNum$");
