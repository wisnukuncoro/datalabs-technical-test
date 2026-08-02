-- Dimensional model (star schema) for sales analytics.
-- Surrogate keys are generated in the MERGE pipelines, never reused from
-- source systems, so history survives a source-side key change or reissue.

CREATE SCHEMA IF NOT EXISTS `{{ params.project_id }}.warehouse`
OPTIONS (location = '{{ params.bq_location }}');

-- dim_date is generated once for a fixed calendar range and never rebuilt by
-- the daily DAG. It is small and queried by equality/range on date_key or
-- full_date, so it is neither partitioned nor clustered.
CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.warehouse.dim_date`
(
    date_key      INT64 NOT NULL,   -- YYYYMMDD, matches fact_sales.date_key
    full_date     DATE NOT NULL,
    day_of_week   INT64 NOT NULL,   -- 1 (Sunday) - 7 (Saturday)
    day_name      STRING NOT NULL,
    day_of_month  INT64 NOT NULL,
    month         INT64 NOT NULL,
    month_name    STRING NOT NULL,
    quarter       INT64 NOT NULL,
    year          INT64 NOT NULL,
    is_weekend    BOOL NOT NULL
)
OPTIONS (
    description = 'Calendar date dimension, pre-populated for a fixed date range.'
);

-- dim_customer is SCD Type 2: city and other profile attributes drift over a
-- customer's lifetime, and cohort/geo analyses need the value that was true
-- at the time of each sale, not the value as of today. customer_key is the
-- surrogate that fact_sales joins against; customer_id is the natural key
-- used to detect changes during the merge.
CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.warehouse.dim_customer`
(
    customer_key          INT64 NOT NULL,
    customer_id           STRING NOT NULL,
    name                  STRING,
    email                 STRING,
    city                  STRING,
    signup_date           DATE,
    effective_start_date  DATE NOT NULL,
    effective_end_date    DATE,
    is_current            BOOL NOT NULL
)
CLUSTER BY customer_id
OPTIONS (
    description = 'SCD Type 2 customer dimension. One row per attribute-change version.'
);

-- dim_product is SCD Type 1 (overwrite in place). The price that mattered for
-- a historical sale is already captured on the fact row (unit_price), so the
-- dimension only needs to reflect the current catalog state, not history.
CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.warehouse.dim_product`
(
    product_key   INT64 NOT NULL,
    product_id    STRING NOT NULL,
    product_name  STRING,
    category      STRING,
    price         NUMERIC,
    updated_at    TIMESTAMP NOT NULL
)
CLUSTER BY category, product_id
OPTIONS (
    description = 'SCD Type 1 product dimension reflecting the current catalog.'
);

-- dim_campaign is SCD Type 1; a campaign's name, channel, and dates are fixed
-- once it runs, so there is nothing to version.
CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.warehouse.dim_campaign`
(
    campaign_key   INT64 NOT NULL,
    campaign_id    STRING NOT NULL,
    campaign_name  STRING,
    channel        STRING,
    start_date     DATE,
    end_date       DATE,
    duration_days  INT64,
    updated_at     TIMESTAMP NOT NULL
)
CLUSTER BY channel
OPTIONS (
    description = 'Marketing campaign dimension. Includes an unknown-member row (campaign_key = -1) for organic sales.'
);
