-- fact_sales grain: one row per transaction line item (transaction_item_id).
-- This is the most granular level available from source data; transaction
-- (order) level metrics are derived by grouping on transaction_id rather than
-- forcing a coarser grain that would throw away basket composition.
--
-- Partitioning: sale_date is a DATE column mirrored from dim_date.full_date
-- specifically so downstream consumers can prune partitions without joining
-- dim_date first. Almost every analytical query on a sales fact filters by a
-- date range, so this is the highest-leverage partitioning key available.
--
-- Clustering: customer_key, product_key and campaign_key are chosen because
-- they are the three dimension keys most commonly used in WHERE/JOIN/GROUP BY
-- for this table (customer cohort analysis, product performance, campaign
-- ROI). BigQuery clustering supports up to 4 columns; sale_date is excluded
-- since it is already the partitioning column.
--
-- require_partition_filter guards against accidental full-table scans on a
-- fact table that is expected to grow into billions of rows.
CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.warehouse.fact_sales`
(
    transaction_item_id      STRING NOT NULL,
    transaction_id           STRING NOT NULL,
    customer_key             INT64 NOT NULL,
    product_key              INT64 NOT NULL,
    campaign_key             INT64 NOT NULL,
    date_key                 INT64 NOT NULL,
    sale_date                DATE NOT NULL,
    quantity                 INT64 NOT NULL,
    unit_price                NUMERIC NOT NULL,
    line_amount               NUMERIC NOT NULL,
    transaction_total_amount  NUMERIC NOT NULL
)
PARTITION BY sale_date
CLUSTER BY customer_key, product_key, campaign_key
OPTIONS (
    require_partition_filter = true,
    description = 'Sales fact at the transaction-line grain. transaction_total_amount is denormalized from the transaction header to support basket-level metrics without a self-join.'
);
