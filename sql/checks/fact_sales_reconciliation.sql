-- Used with BigQueryCheckOperator: confirms every transaction item landed in
-- staging for this date range made it into fact_sales, catching silent join
-- drops (e.g. a transaction item whose customer or product has no matching
-- dimension row and is therefore excluded by an inner join upstream).
WITH staging_count AS (
    SELECT COUNT(*) AS item_count
    FROM `{{ params.project_id }}.staging.stg_transaction_items` AS ti
    JOIN `{{ params.project_id }}.staging.stg_transactions` AS t
        ON ti.transaction_id = t.transaction_id
    WHERE t.transaction_date BETWEEN DATE('{{ params.start_date }}') AND DATE('{{ params.end_date }}')
),
fact_count AS (
    SELECT COUNT(*) AS item_count
    FROM `{{ params.project_id }}.warehouse.fact_sales`
    WHERE sale_date BETWEEN DATE('{{ params.start_date }}') AND DATE('{{ params.end_date }}')
)
SELECT staging_count.item_count = fact_count.item_count
FROM staging_count, fact_count;
