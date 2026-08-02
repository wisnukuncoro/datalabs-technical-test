-- Staging layer: raw landing tables loaded 1:1 from the GCS extract.
-- Ingestion-time partitioning (_PARTITIONTIME) is used instead of a managed
-- timestamp column: each GCSToBigQueryOperator load targets a single day's
-- partition decorator (table$YYYYMMDD) with WRITE_TRUNCATE, so a rerun of a
-- given logical date replaces exactly that day's rows and nothing else.
-- A 30-day partition expiration keeps the transient landing data from
-- accumulating indefinitely; it is not the system of record.

CREATE SCHEMA IF NOT EXISTS `{{ params.project_id }}.staging`
OPTIONS (location = '{{ params.bq_location }}');

CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.staging.stg_customers`
(
    customer_id STRING NOT NULL,
    name        STRING,
    email       STRING,
    city        STRING,
    signup_date DATE
)
PARTITION BY _PARTITIONDATE
OPTIONS (
    partition_expiration_days = 30,
    description = 'Raw customer records landed from the OLTP extract, one ingestion-time partition per load date.'
);

CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.staging.stg_products`
(
    product_id   STRING NOT NULL,
    product_name STRING,
    category     STRING,
    price        NUMERIC
)
PARTITION BY _PARTITIONDATE
OPTIONS (
    partition_expiration_days = 30,
    description = 'Raw product catalog records landed from the OLTP extract.'
);

CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.staging.stg_transactions`
(
    transaction_id   STRING NOT NULL,
    customer_id      STRING,
    transaction_date DATE,
    total_amount     NUMERIC,
    campaign_id      STRING
)
PARTITION BY _PARTITIONDATE
OPTIONS (
    partition_expiration_days = 30,
    description = 'Raw transaction headers landed from the OLTP extract.'
);

CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.staging.stg_transaction_items`
(
    transaction_item_id STRING NOT NULL,
    transaction_id      STRING,
    product_id           STRING,
    quantity             INT64,
    price                NUMERIC
)
PARTITION BY _PARTITIONDATE
OPTIONS (
    partition_expiration_days = 30,
    description = 'Raw transaction line items landed from the OLTP extract.'
);

CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.staging.stg_marketing_campaigns`
(
    campaign_id   STRING NOT NULL,
    campaign_name STRING,
    start_date    DATE,
    end_date      DATE,
    channel       STRING
)
PARTITION BY _PARTITIONDATE
OPTIONS (
    partition_expiration_days = 30,
    description = 'Raw marketing campaign records landed from the OLTP extract.'
);
