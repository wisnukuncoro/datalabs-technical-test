"""Populates dim_date over a fixed calendar range.

Unlike the fact and other dimension tables, the date dimension does not
depend on any extract landing in GCS - it is derived purely from a calendar
range, so it is not part of the daily DAG. This DAG is triggered manually (or
on a yearly schedule) whenever the range needs extending; the underlying SQL
is idempotent, so re-running it after the range already exists is a no-op.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = Variable.get("gcp_project_id", default_var="datalabs-technical-test")
BQ_LOCATION = Variable.get("bq_location", default_var="asia-southeast2")
DIM_DATE_START = Variable.get("dim_date_start", default_var="2022-01-01")
DIM_DATE_END = Variable.get("dim_date_end", default_var="2030-12-31")

SQL_DIR = "/opt/airflow/sql"


@dag(
    dag_id="dim_date_maintenance",
    description="Idempotently (re)generates dim_date over a fixed calendar range",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    template_searchpath=[f"{SQL_DIR}/warehouse"],
    tags=["ecommerce", "bigquery", "warehouse", "maintenance"],
)
def dim_date_maintenance():
    BigQueryInsertJobOperator(
        task_id="generate_dim_date",
        configuration={"query": {"query": "generate_dim_date.sql", "useLegacySql": False}},
        params={
            "project_id": PROJECT_ID,
            "bq_location": BQ_LOCATION,
            "dim_date_start": DIM_DATE_START,
            "dim_date_end": DIM_DATE_END,
        },
        location=BQ_LOCATION,
    )


dim_date_maintenance()
