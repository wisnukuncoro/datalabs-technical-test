-- SCD Type 2 merge for dim_customer, run in two passes because a single
-- MERGE cannot both close an existing version and insert its replacement for
-- the same natural key.
--
-- Pass 1: close out current versions whose tracked attributes changed.
MERGE `{{ params.project_id }}.warehouse.dim_customer` AS target
USING (
    SELECT customer_id, name, email, city, signup_date
    FROM `{{ params.project_id }}.staging.stg_customers`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _PARTITIONTIME DESC) = 1
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND (
    target.name != source.name OR
    target.email != source.email OR
    target.city != source.city
) THEN
    UPDATE SET
        target.effective_end_date = CURRENT_DATE() - 1,
        target.is_current = FALSE;

-- Pass 2: insert brand-new customers and the new version of any customer
-- closed out above (pass 1 already flipped is_current to FALSE for those,
-- so the LEFT JOIN below no longer finds a current row for them either).
INSERT INTO `{{ params.project_id }}.warehouse.dim_customer`
(customer_key, customer_id, name, email, city, signup_date, effective_start_date, effective_end_date, is_current)
SELECT
    FARM_FINGERPRINT(CONCAT(source.customer_id, '|', CAST(CURRENT_DATE() AS STRING))),
    source.customer_id,
    source.name,
    source.email,
    source.city,
    source.signup_date,
    CURRENT_DATE(),
    CAST(NULL AS DATE),
    TRUE
FROM (
    SELECT customer_id, name, email, city, signup_date
    FROM `{{ params.project_id }}.staging.stg_customers`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _PARTITIONTIME DESC) = 1
) AS source
LEFT JOIN `{{ params.project_id }}.warehouse.dim_customer` AS current_dim
    ON source.customer_id = current_dim.customer_id AND current_dim.is_current = TRUE
WHERE current_dim.customer_id IS NULL;
