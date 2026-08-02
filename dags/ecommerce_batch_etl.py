"""Daily batch ETL: GCS landing zone -> BigQuery staging -> BigQuery star schema.

Each run processes a single logical date (the DAG's data interval). Staging
loads target that date's ingestion-time partition with WRITE_TRUNCATE, and the
dimension/fact merges are scoped to the same date, so a run is safe to retry
or replay through `airflow dags backfill` without producing duplicates.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ID = Variable.get("gcp_project_id", default_var="ecommerce-analytics")
BQ_LOCATION = Variable.get("bq_location", default_var="asia-southeast2")
LANDING_BUCKET = Variable.get("gcs_landing_bucket", default_var="ecommerce-analytics-landing")

SQL_DIR = "/opt/airflow/sql"
BASE_PARAMS = {"project_id": PROJECT_ID, "bq_location": BQ_LOCATION}

STAGING_TABLE_SCHEMAS = {
    "customers": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "signup_date", "type": "DATE", "mode": "NULLABLE"},
    ],
    "products": [
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "NUMERIC", "mode": "NULLABLE"},
    ],
    "transactions": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "transaction_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "total_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
    ],
    "transaction_items": [
        {"name": "transaction_item_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "price", "type": "NUMERIC", "mode": "NULLABLE"},
    ],
    "marketing_campaigns": [
        {"name": "campaign_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "start_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "end_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "channel", "type": "STRING", "mode": "NULLABLE"},
    ],
}


def _gcs_source_object(table: str) -> str:
    return "landing/" + table + "/{{ ds_nodash }}/" + table + ".csv"


def _staging_destination(table: str) -> str:
    return PROJECT_ID + ".staging.stg_" + table + "${{ ds_nodash }}"


def _sql_job(task_id: str, filename: str, extra_params: dict | None = None) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": {"query": filename, "useLegacySql": False}},
        params={**BASE_PARAMS, **(extra_params or {})},
        location=BQ_LOCATION,
    )


@dag(
    dag_id="ecommerce_batch_etl",
    description="Loads e-commerce sales data from GCS into a BigQuery star schema",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    template_searchpath=[
        f"{SQL_DIR}/staging",
        f"{SQL_DIR}/warehouse",
        f"{SQL_DIR}/checks",
    ],
    tags=["ecommerce", "bigquery", "warehouse"],
)
def ecommerce_batch_etl():

    create_staging_tables = _sql_job("create_staging_tables", "create_staging_tables.sql")
    create_dimension_tables = _sql_job("create_dimension_tables", "create_dimension_tables.sql")
    create_fact_table = _sql_job("create_fact_table", "create_fact_tables.sql")

    with TaskGroup(group_id="load_staging") as load_staging:
        for table, schema in STAGING_TABLE_SCHEMAS.items():
            GCSToBigQueryOperator(
                task_id=f"load_{table}",
                bucket=LANDING_BUCKET,
                source_objects=[_gcs_source_object(table)],
                destination_project_dataset_table=_staging_destination(table),
                schema_fields=schema,
                source_format="CSV",
                skip_leading_rows=1,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_NEVER",
                autodetect=False,
            )

    with TaskGroup(group_id="check_staging_landed") as check_staging_landed:
        for table in STAGING_TABLE_SCHEMAS:
            BigQueryCheckOperator(
                task_id=f"check_{table}_row_count",
                sql="staging_row_count_positive.sql",
                use_legacy_sql=False,
                params={**BASE_PARAMS, "table_name": f"stg_{table}", "check_date": "{{ ds }}"},
            )

    with TaskGroup(group_id="refresh_dimensions") as refresh_dimensions:
        _sql_job("merge_dim_customer", "merge_dim_customer.sql")
        _sql_job("merge_dim_product", "merge_dim_product.sql")
        _sql_job("merge_dim_campaign", "merge_dim_campaign.sql")

    load_fact_sales = _sql_job(
        "load_fact_sales",
        "merge_fact_sales.sql",
        extra_params={"start_date": "{{ ds }}", "end_date": "{{ ds }}"},
    )

    check_fact_reconciliation = BigQueryCheckOperator(
        task_id="check_fact_sales_reconciliation",
        sql="fact_sales_reconciliation.sql",
        use_legacy_sql=False,
        params={**BASE_PARAMS, "start_date": "{{ ds }}", "end_date": "{{ ds }}"},
    )

    (
        [create_staging_tables, create_dimension_tables, create_fact_table]
        >> load_staging
        >> check_staging_landed
        >> refresh_dimensions
        >> load_fact_sales
        >> check_fact_reconciliation
    )


ecommerce_batch_etl()
