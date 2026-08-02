-- Used with BigQueryCheckOperator: fails the task if today's partition of a
-- staging table landed zero rows, catching a silently empty or failed
-- upstream extract before it propagates into the warehouse merges.
SELECT COUNT(*) > 0
FROM `{{ params.project_id }}.staging.{{ params.table_name }}`
WHERE _PARTITIONDATE = DATE('{{ params.check_date }}');
