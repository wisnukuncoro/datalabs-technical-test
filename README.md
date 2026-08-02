# E-Commerce Analytics Pipeline

Disclaimer:

**This project is AI-assisted (using Claude Code) as part of the development workflow.**

All architectural decisions, technology choices, code reviews, and final implementations are my own. I fully understand every part of the codebase and can explain, modify, or extend it without relying on AI.

I believe modern software engineering is about delivering reliable, maintainable, and scalable systems—not manually typing every line of code. AI is used as a productivity tool to accelerate implementation, while engineering principles, system design, and technical ownership remain the developer's responsibility.


About the project

A batch and streaming data engineering pipeline for e-commerce sales analytics:
Python-generated sample data lands in a GCS bucket, Apache Airflow orchestrates
loading it into BigQuery, and a dimensional (star schema) model in BigQuery
serves analytics. A bonus real-time pipeline aggregates a dummy transaction
stream through Kafka.

**Author:** Kuncoro Wisnu Jati ([wisnujati29@gmail.com](mailto:wisnujati29@gmail.com))

## Contents

- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Data model](#data-model)
- [Setup](#setup)
- [Data engineering concepts and why they're here](#data-engineering-concepts-and-why-theyre-here)
- [Streaming pipeline (bonus)](#streaming-pipeline-bonus)
- [Assumptions](#assumptions)

## Architecture

### Batch pipeline

[diagrams/batch_architecture.drawio](diagrams/batch_architecture.drawio) —
GitHub renders `.drawio` files inline in the file browser; click through to
view it, or open it in [diagrams.net](https://app.diagrams.net) to edit.

An OLTP-style source is extracted into flat files, landed in a GCS bucket
partitioned by day, then loaded into BigQuery in two layers: a staging layer
that mirrors the source schema, and a warehouse layer that reshapes it into a
star schema. Airflow owns scheduling, retries, and backfills; it does not
transform data itself — every transformation is a SQL statement executed by
BigQuery, so the transformation logic is portable, testable outside Airflow,
and reviewable as plain SQL rather than buried in operator code.

The dimensional model itself is diagrammed separately in
[diagrams/erd.drawio](diagrams/erd.drawio) — see [Data model](#data-model).

### Streaming pipeline

[diagrams/streaming_architecture.drawio](diagrams/streaming_architecture.drawio)

## Repository layout

```
dags/                       Airflow DAGs
  ecommerce_batch_etl.py       daily batch pipeline (GCS -> staging -> warehouse)
  dim_date_maintenance.py      one-off/low-frequency dim_date generation

sql/
  staging/                     staging table DDL
  warehouse/                   dimension/fact DDL and MERGE transformation logic
  checks/                      data quality check queries
  streaming/                   DDL for the streaming sink table

src/
  data_generation/              sample data generator (Faker-based)
  etl/                          SQL templating helper + one-time historical bootstrap loader
  streaming/                    Kafka producer, windowed-aggregation consumer, sinks

data/sample/                  generated sample CSVs (customers, products, transactions,
                               transaction_items, marketing_campaigns), 1,000+ rows each

diagrams/                     draw.io architecture and ERD diagrams

tests/                        unit tests for the SQL templating helper and the
                               streaming windowing logic

docker-compose.yml            local Airflow (LocalExecutor + Postgres)
docker-compose.streaming.yml  local Kafka-compatible broker (Redpanda) for the streaming demo
```

## Data model

Source tables (as required by the test) are extracted as-is into staging,
then reshaped into a star schema for analytics:

- **Fact:** `fact_sales` — one row per transaction line item.
- **Dimensions:** `dim_customer`, `dim_product`, `dim_campaign`, `dim_date`.

Full DDL: [`sql/warehouse/create_dimension_tables.sql`](sql/warehouse/create_dimension_tables.sql),
[`sql/warehouse/create_fact_tables.sql`](sql/warehouse/create_fact_tables.sql).
ERD: [`diagrams/erd.drawio`](diagrams/erd.drawio).

### Modeling choices

**Grain.** `fact_sales` is grained at the transaction line item
(`transaction_item_id`), not the transaction. This is the most granular level
the source data offers; order-level metrics (basket size, order count,
average order value) are derived by grouping on `transaction_id` rather than
forcing a coarser grain that would discard basket composition.

**`dim_customer` is Type 2 (versioned), `dim_product`/`dim_campaign` are Type
1 (overwritten).** A customer's city and profile attributes drift over their
lifetime, and cohort/geo analyses need the value that was true when a sale
happened, not today's value — so `dim_customer` keeps every version with
`effective_start_date` / `effective_end_date` / `is_current`, and
`fact_sales` joins to the version that was active on the sale date. Products
and campaigns don't need this: a product's historical price is already
captured on the fact row (`unit_price`), so the dimension only needs to
reflect the current catalog; a campaign's attributes are fixed once it runs.

**Campaign attribution is denormalized onto `transactions`, not modeled as a
many-to-many bridge.** The source schema doesn't specify how a sale relates
to a campaign, so the sample data adds a nullable `campaign_id` on each
transaction (one campaign attributed per sale, or none for organic sales).
`dim_campaign` includes a permanent unknown-member row (`campaign_key = -1`)
for organic sales, so `campaign_key` on the fact table is never null and
every join is an inner join.

**`transaction_total_amount` is denormalized onto every line of `fact_sales`.**
The transaction header total is repeated on each of its line items rather
than requiring a self-join back to a transaction-grain table whenever a
query needs basket-level totals alongside line-level detail. This is a
deliberate, small amount of redundancy traded for query simplicity — a
standard fact-table pattern once the grain has been chosen.

**`dim_date` is neither partitioned nor clustered.** It's a few thousand rows
spanning years, and it's queried by equality/range on `date_key` — small
enough that partitioning would add overhead (extra metadata, more partitions
to prune) without a measurable scan-cost benefit. Partitioning and clustering
are applied where they pay for themselves: `fact_sales`.

### Partitioning and clustering (BigQuery)

- **Staging (`stg_*`):** ingestion-time partitioned (`PARTITION BY
  _PARTITIONDATE`) with a 30-day expiration. Staging is transient landing
  data, not the system of record, so it doesn't need to be retained
  indefinitely, and ingestion-time partitioning means the loader never has to
  manage a timestamp column itself — each `GCSToBigQueryOperator` load
  targets a specific day's partition via a decorator
  (`stg_customers$20260115`) with `WRITE_TRUNCATE`, so re-running a given
  logical date replaces exactly that day's rows.
- **`fact_sales`:** partitioned by `sale_date` (a `DATE` column mirrored from
  `dim_date.full_date`) and clustered by `customer_key, product_key,
  campaign_key`. Almost every analytical query on a sales fact filters by a
  date range, so partitioning on it is the single highest-leverage choice
  available; the three clustering columns are the dimension keys most
  commonly used in `WHERE`/`JOIN`/`GROUP BY` for this table (customer cohort
  analysis, product performance, campaign ROI). `require_partition_filter =
  true` is set so a query without a date filter fails fast at query-compile
  time instead of silently scanning — and scanning the bill for — the whole
  table as it grows.
- **`dim_customer`/`dim_product`:** clustered (not partitioned) on their
  natural key / most common filter column, since they're small enough that
  clustering alone is sufficient to make key lookups and `MERGE` joins cheap.

## Setup

### 1. Generate the sample data

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 src/data_generation/generate_sample_data.py
```

Writes five CSVs to `data/sample/` (already generated and committed in this
repo): `customers.csv` (1,200 rows), `products.csv` (180 rows),
`marketing_campaigns.csv` (24 rows), `transactions.csv` (6,000 rows),
`transaction_items.csv` (~18,000 rows). Generation is seeded, so it is
reproducible. Line item totals reconcile exactly with
`transactions.total_amount` by construction.

### 2. Provision BigQuery

You need a GCP project with the BigQuery API enabled and a service account
key with the `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` roles.

```bash
mkdir -p gcp && cp /path/to/your-service-account.json gcp/service-account.json
```

For a quick historical seed load without standing up Airflow or GCS at all,
run the one-time bootstrap script directly against BigQuery — it creates all
staging/warehouse tables, loads the five CSVs, and runs the same
transformation SQL the DAG uses, scoped to the full historical date range:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=gcp/service-account.json
python3 -m src.etl.bootstrap_historical_load --project-id YOUR_GCP_PROJECT --bq-location asia-southeast2
```

### 3. Run the Airflow DAG (steady-state, daily)

```bash
export GCP_PROJECT_ID=YOUR_GCP_PROJECT
export BQ_LOCATION=asia-southeast2
export GCS_LANDING_BUCKET=your-landing-bucket
docker compose up airflow-init
docker compose up -d
```

Airflow UI: [http://localhost:8080](http://localhost:8080) (`admin` / `admin`).
`ecommerce_batch_etl` is scheduled `@daily` and expects that day's extract at
`gs://<bucket>/landing/<table>/<yyyymmdd>/<table>.csv` for each of the five
tables. `dim_date_maintenance` is unscheduled (`schedule=None`) — trigger it
manually the first time, and again whenever the generated calendar range
needs extending.

To reprocess a historical range through Airflow itself instead of the
bootstrap script (e.g. to demonstrate backfill), use Airflow's native
backfill command — the DAG is idempotent per logical date, so replaying a
range is safe:

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill ecommerce_batch_etl -s 2024-01-01 -e 2024-01-31
```

### 4. Run the tests

```bash
pip install -r requirements-dev.txt
pytest
```

## Data engineering concepts and why they're here

**Idempotent, replayable batch loads.** Every load and merge in this pipeline
is scoped to a logical date and safe to re-run: staging loads replace a
single ingestion-time partition (`WRITE_TRUNCATE` on a partition decorator),
and the `fact_sales` `MERGE` is bounded by a `start_date`/`end_date` range
that becomes part of its `ON` clause. That means a failed run, a manual
retry, or an `airflow dags backfill` over a historical range all produce the
same result as a single successful run — no duplicate rows, no manual
cleanup.

**Orchestration vs. transformation are separate concerns.** Airflow decides
*when* and *in what order* things run, and retries/alerts when they don't;
BigQuery does the actual data transformation via plain `.sql` files
(`template_searchpath` + `BigQueryInsertJobOperator`). This keeps the
transformation logic testable and reviewable independent of Airflow, and
means the same SQL can run from the DAG, from the standalone bootstrap
script, or from the `bq` CLI.

**Dimensional modeling (star schema) over a normalized OLTP-shaped
warehouse.** Analytical queries (revenue by category by month, campaign ROI,
customer cohort retention) become simple aggregations over a small number of
joins from one fact to a few conformed dimensions, instead of multi-hop joins
across normalized tables. This is the classic trade of write-side complexity
(the `MERGE` logic) for read-side simplicity, which is the right trade for a
warehouse that's read far more often than it's written.

**Slowly Changing Dimensions.** SCD Type 2 on `dim_customer` preserves
history so a metric computed "as of" a past date reflects the world as it
was then, not as it is now — without it, a customer who moved city would
silently rewrite history every time a report re-aggregated past sales by
city. SCD Type 1 elsewhere avoids paying that versioning cost where nothing
depends on the history.

**Partitioning and clustering as a cost and performance control, not just an
organizational one.** BigQuery bills (and scales query latency) by bytes
scanned. Partition pruning on `sale_date` and `require_partition_filter`
directly bound both cost and blast radius as `fact_sales` grows; clustering
narrows the scan further within a partition for the dimension keys queries
actually filter and join on.

**Data quality checks as pipeline gates, not an afterthought.**
`BigQueryCheckOperator` tasks run inline in the DAG: a staging load that
lands zero rows, or a `fact_sales` merge whose row count doesn't reconcile
against staging for the same date range, fails the DAG run before bad data
reaches anything downstream, rather than surfacing as a silent discrepancy
in a dashboard days later.

## Streaming pipeline (bonus)

A minimal real-time pipeline: `src/streaming/producer.py` emits dummy
transaction events to a Kafka topic; `src/streaming/consumer.py` aggregates
them into one-minute tumbling windows and emits per-window transaction count
and total amount.

```bash
docker compose -f docker-compose.streaming.yml up -d
source .venv/bin/activate
python3 src/streaming/producer.py &
python3 src/streaming/consumer.py
```

Add `--bq-project-id` and `--bq-table` to `consumer.py` to also stream
results into BigQuery (create the sink table first via
[`sql/streaming/create_streaming_table.sql`](sql/streaming/create_streaming_table.sql));
otherwise it only prints to the console.

**Event-time windowing with a watermark, not processing-time windowing.**
Each event carries its own `event_time`; the consumer buckets by event time
and only flushes a window once the watermark (the latest event time seen,
minus a small allowed-lateness grace period) has moved past the window's
end. This means a message that arrives slightly out of order still lands in
the correct minute instead of being counted in whichever window happened to
be "current" when it was processed — the same problem real systems solve
with Kafka Streams, Flink, or Beam's windowing model, implemented here at a
scale simple enough to read in one file.

**Kafka (via Redpanda) as the transport, decoupling producer from
consumer.** The producer and consumer don't know about each other or need to
run at the same rate; the broker durably buffers events between them, so the
consumer can be restarted, scaled, or replaced without losing events or
coordinating directly with the producer. Redpanda is used in place of
Apache Kafka + Zookeeper for the local demo because it's Kafka-API
compatible in a single container, with no functional difference to the
producer/consumer code, which speaks the standard Kafka protocol via
`kafka-python`.

## Assumptions

- The five source tables are treated as a daily extract from an upstream
  OLTP system landing in GCS; the sample CSVs represent a one-time
  historical snapshot, loaded via the bootstrap script rather than by
  replaying thousands of synthetic daily DAG runs (see
  [Setup](#setup)).
- `transactions.campaign_id` (nullable) is an addition beyond the tables
  specified in the brief, added so that `fact_sales` has a meaningful,
  non-trivial relationship to `dim_campaign` to model and query.
  Approximately 18% of generated transactions are attributed to a campaign
  active on their transaction date; the rest are organic.
- All monetary amounts are in Indonesian Rupiah (IDR) with no currency
  conversion; city names are Indonesian cities. BigQuery location defaults
  to `asia-southeast2` (Jakarta).
- A service-account-based connection (`GOOGLE_APPLICATION_CREDENTIALS`) is
  assumed for both Airflow and the standalone scripts, rather than a
  pre-configured Airflow connection, to keep local setup to a single
  environment variable.
