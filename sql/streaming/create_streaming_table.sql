-- Sink table for the real-time pipeline's per-minute window aggregates.
-- Kept in its own dataset since it is populated by streaming inserts on a
-- different cadence than the batch warehouse tables, and has no dimensional
-- model of its own - it is a thin operational metric, not an analytical fact.
CREATE SCHEMA IF NOT EXISTS `{{ params.project_id }}.streaming`
OPTIONS (location = '{{ params.bq_location }}');

CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.streaming.transaction_window_counts`
(
    window_start      TIMESTAMP NOT NULL,
    transaction_count INT64 NOT NULL,
    total_amount      NUMERIC NOT NULL
)
PARTITION BY DATE(window_start)
OPTIONS (
    partition_expiration_days = 90,
    description = 'Per-minute tumbling-window transaction counts from the real-time pipeline.'
);
