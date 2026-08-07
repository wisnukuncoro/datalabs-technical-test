"""One-time historical seed load, run outside of Airflow.

The scheduled DAG (dags/ecommerce_batch_etl.py) assumes each run receives one
day's worth of new/changed records at a date-partitioned GCS path. The sample
dataset instead represents four years of pre-existing history in a single
flat export per table, which is not the shape the daily DAG expects. Rather
than replaying ~1,460 synthetic daily DAG runs against split files, this
script loads that historical snapshot directly into BigQuery and runs the
same warehouse SQL used by the DAG, scoped to the full historical date range.

Usage:
    python -m src.etl.bootstrap_historical_load --project-id datalabs-technical-test
"""

from __future__ import annotations

import argparse
from pathlib import Path

from google.cloud import bigquery

from src.data_generation.generate_sample_data import SIGNUP_WINDOW_START, TRANSACTION_WINDOW_END
from src.etl.sql_templates import render_sql

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "sample"

STAGING_SCHEMAS = {
    "stg_customers": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("signup_date", "DATE"),
    ],
    "stg_products": [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("price", "NUMERIC"),
    ],
    "stg_transactions": [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("transaction_date", "DATE"),
        bigquery.SchemaField("total_amount", "NUMERIC"),
        bigquery.SchemaField("campaign_id", "STRING"),
    ],
    "stg_transaction_items": [
        bigquery.SchemaField("transaction_item_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transaction_id", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("price", "NUMERIC"),
    ],
    "stg_marketing_campaigns": [
        bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("channel", "STRING"),
    ],
}

CSV_BY_TABLE = {
    "stg_customers": "customers.csv",
    "stg_products": "products.csv",
    "stg_transactions": "transactions.csv",
    "stg_transaction_items": "transaction_items.csv",
    "stg_marketing_campaigns": "marketing_campaigns.csv",
}


def run_ddl(client: bigquery.Client, project_id: str, bq_location: str) -> None:
    for relative_path in (
        "staging/create_staging_tables.sql",
        "warehouse/create_dimension_tables.sql",
        "warehouse/create_fact_tables.sql",
    ):
        sql = render_sql(relative_path, project_id=project_id, bq_location=bq_location)
        client.query(sql, location=bq_location).result()


def load_staging_tables(client: bigquery.Client, project_id: str, bq_location: str) -> None:
    for table, schema in STAGING_SCHEMAS.items():
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        csv_path = DATA_DIR / CSV_BY_TABLE[table]
        with csv_path.open("rb") as csv_file:
            job = client.load_table_from_file(
                csv_file,
                f"{project_id}.staging.{table}",
                job_config=job_config,
                location=bq_location,
            )
        job.result()
        print(f"loaded {job.output_rows} rows into {table}")


def run_warehouse_merges(client: bigquery.Client, project_id: str, bq_location: str) -> None:
    start_date = SIGNUP_WINDOW_START.isoformat()
    end_date = TRANSACTION_WINDOW_END.isoformat()

    client.query(
        render_sql(
            "warehouse/generate_dim_date.sql",
            project_id=project_id,
            bq_location=bq_location,
            dim_date_start=start_date,
            dim_date_end=end_date,
        ),
        location=bq_location,
    ).result()

    for relative_path in (
        "warehouse/merge_dim_customer.sql",
        "warehouse/merge_dim_product.sql",
        "warehouse/merge_dim_campaign.sql",
    ):
        client.query(
            render_sql(relative_path, project_id=project_id, bq_location=bq_location),
            location=bq_location,
        ).result()

    client.query(
        render_sql(
            "warehouse/merge_fact_sales.sql",
            project_id=project_id,
            bq_location=bq_location,
            start_date=start_date,
            end_date=end_date,
        ),
        location=bq_location,
    ).result()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--bq-location", default="asia-southeast2")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project_id)

    run_ddl(client, args.project_id, args.bq_location)
    load_staging_tables(client, args.project_id, args.bq_location)
    run_warehouse_merges(client, args.project_id, args.bq_location)


if __name__ == "__main__":
    main()
