-- Loads fact_sales for a single logical date range (start_date/end_date are
-- both set to the DAG's logical date {{ ds }} in normal daily operation, and
-- widened when backfilling via `airflow dags backfill`). Scoping both the
-- source filter and the MERGE ON clause to this range keeps the statement
-- compatible with require_partition_filter and avoids rescanning the whole
-- fact table on every run.
--
-- Point-in-time dimension joins: dim_customer is SCD Type 2, so the join
-- picks the customer version that was effective on the sale date rather than
-- the customer's current attributes. dim_campaign is left-joined and falls
-- back to the unknown-member row (-1) for organic sales with no attributed
-- campaign_id.
MERGE `{{ params.project_id }}.warehouse.fact_sales` AS target
USING (
    SELECT
        ti.transaction_item_id,
        ti.transaction_id,
        dc.customer_key,
        dp.product_key,
        COALESCE(dcamp.campaign_key, -1) AS campaign_key,
        CAST(FORMAT_DATE('%Y%m%d', t.transaction_date) AS INT64) AS date_key,
        t.transaction_date AS sale_date,
        ti.quantity,
        ti.price AS unit_price,
        ti.quantity * ti.price AS line_amount,
        t.total_amount AS transaction_total_amount
    FROM `{{ params.project_id }}.staging.stg_transaction_items` AS ti
    JOIN `{{ params.project_id }}.staging.stg_transactions` AS t
        ON ti.transaction_id = t.transaction_id
    JOIN `{{ params.project_id }}.warehouse.dim_customer` AS dc
        ON t.customer_id = dc.customer_id
        AND t.transaction_date BETWEEN dc.effective_start_date AND COALESCE(dc.effective_end_date, DATE '9999-12-31')
    JOIN `{{ params.project_id }}.warehouse.dim_product` AS dp
        ON ti.product_id = dp.product_id
    LEFT JOIN `{{ params.project_id }}.warehouse.dim_campaign` AS dcamp
        ON t.campaign_id = dcamp.campaign_id
    WHERE t.transaction_date BETWEEN DATE('{{ params.start_date }}') AND DATE('{{ params.end_date }}')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ti.transaction_item_id ORDER BY ti._PARTITIONTIME DESC) = 1
) AS source
ON target.transaction_item_id = source.transaction_item_id
    AND target.sale_date BETWEEN DATE('{{ params.start_date }}') AND DATE('{{ params.end_date }}')
WHEN MATCHED THEN
    UPDATE SET
        target.customer_key = source.customer_key,
        target.product_key = source.product_key,
        target.campaign_key = source.campaign_key,
        target.date_key = source.date_key,
        target.quantity = source.quantity,
        target.unit_price = source.unit_price,
        target.line_amount = source.line_amount,
        target.transaction_total_amount = source.transaction_total_amount
WHEN NOT MATCHED THEN
    INSERT (
        transaction_item_id, transaction_id, customer_key, product_key, campaign_key,
        date_key, sale_date, quantity, unit_price, line_amount, transaction_total_amount
    )
    VALUES (
        source.transaction_item_id, source.transaction_id, source.customer_key, source.product_key,
        source.campaign_key, source.date_key, source.sale_date, source.quantity, source.unit_price,
        source.line_amount, source.transaction_total_amount
    );
