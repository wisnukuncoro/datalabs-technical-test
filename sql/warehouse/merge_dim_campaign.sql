-- SCD Type 1 merge for dim_campaign, plus a permanent unknown-member row
-- (campaign_key = -1) that fact_sales points to for organic, non-attributed
-- sales instead of leaving a nullable foreign key on the fact table.
MERGE `{{ params.project_id }}.warehouse.dim_campaign` AS target
USING (
    SELECT campaign_id, campaign_name, start_date, end_date, channel
    FROM `{{ params.project_id }}.staging.stg_marketing_campaigns`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _PARTITIONTIME DESC) = 1
) AS source
ON target.campaign_id = source.campaign_id
WHEN MATCHED THEN
    UPDATE SET
        target.campaign_name = source.campaign_name,
        target.channel = source.channel,
        target.start_date = source.start_date,
        target.end_date = source.end_date,
        target.duration_days = DATE_DIFF(source.end_date, source.start_date, DAY),
        target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (campaign_key, campaign_id, campaign_name, channel, start_date, end_date, duration_days, updated_at)
    VALUES (
        FARM_FINGERPRINT(source.campaign_id),
        source.campaign_id,
        source.campaign_name,
        source.channel,
        source.start_date,
        source.end_date,
        DATE_DIFF(source.end_date, source.start_date, DAY),
        CURRENT_TIMESTAMP()
    );

MERGE `{{ params.project_id }}.warehouse.dim_campaign` AS target
USING (SELECT -1 AS campaign_key) AS source
ON target.campaign_key = source.campaign_key
WHEN NOT MATCHED THEN
    INSERT (campaign_key, campaign_id, campaign_name, channel, start_date, end_date, duration_days, updated_at)
    VALUES (-1, 'UNKNOWN', 'No Campaign (Organic)', 'Organic', NULL, NULL, NULL, CURRENT_TIMESTAMP());
