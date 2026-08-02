-- One-time / low-frequency population of dim_date over a fixed calendar
-- range. Idempotent via the NOT IN filter, so re-running it after the range
-- has already been generated is a no-op. Not part of the daily DAG; run
-- manually (or on a yearly schedule) when the range needs extending.
INSERT INTO `{{ params.project_id }}.warehouse.dim_date`
(date_key, full_date, day_of_week, day_name, day_of_month, month, month_name, quarter, year, is_weekend)
SELECT
    CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) AS date_key,
    d AS full_date,
    EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
    FORMAT_DATE('%A', d) AS day_name,
    EXTRACT(DAY FROM d) AS day_of_month,
    EXTRACT(MONTH FROM d) AS month,
    FORMAT_DATE('%B', d) AS month_name,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(DAYOFWEEK FROM d) IN (1, 7) AS is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY('{{ params.dim_date_start }}', '{{ params.dim_date_end }}')) AS d
WHERE CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) NOT IN (
    SELECT date_key FROM `{{ params.project_id }}.warehouse.dim_date`
);
