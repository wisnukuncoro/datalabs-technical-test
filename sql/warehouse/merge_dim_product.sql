-- SCD Type 1 merge for dim_product: overwrite in place, surrogate key stable
-- across updates so historical fact rows keep pointing at the same product.
MERGE `{{ params.project_id }}.warehouse.dim_product` AS target
USING (
    SELECT product_id, product_name, category, price
    FROM `{{ params.project_id }}.staging.stg_products`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _PARTITIONTIME DESC) = 1
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET
        target.product_name = source.product_name,
        target.category = source.category,
        target.price = source.price,
        target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_key, product_id, product_name, category, price, updated_at)
    VALUES (
        FARM_FINGERPRINT(source.product_id),
        source.product_id,
        source.product_name,
        source.category,
        source.price,
        CURRENT_TIMESTAMP()
    );
