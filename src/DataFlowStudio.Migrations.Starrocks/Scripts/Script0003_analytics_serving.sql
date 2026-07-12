-- analytics — the StarRocks real-time serving surface (schemas/dataflow-studio/README.md lists dwh +
-- analytics as owned; the authored DDL details the star, and serving objects are layered on top).
-- The serving layer starts as a thin, query-friendly logical view over the current customer
-- dimension; the sink slices grow it (serving aggregates / async materialized views) as facts land.

CREATE VIEW analytics.dim_customer_current AS
SELECT
    customer_sk,
    customer_id,
    customer_code,
    display_name,
    email,
    preferred_locale,
    status,
    lifetime_value_usd
FROM dwh.dim_customer
WHERE is_current = TRUE;
