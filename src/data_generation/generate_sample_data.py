"""Generates a referentially consistent sample dataset for the e-commerce pipeline.

Simulates a five-year-old OLTP export: customers, products, transactions,
transaction line items, and marketing campaigns. Output lands in data/sample/
as CSV, mirroring the landing-zone format the Airflow DAG ingests from GCS.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "sample"

NUM_CUSTOMERS = 1_200
NUM_PRODUCTS = 180
NUM_CAMPAIGNS = 24
NUM_TRANSACTIONS = 6_000
MAX_ITEMS_PER_TRANSACTION = 5

SIGNUP_WINDOW_START = date(2022, 1, 1)
SIGNUP_WINDOW_END = date(2025, 6, 30)
TRANSACTION_WINDOW_END = date(2025, 12, 31)

CITIES = [
    "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar",
    "Palembang", "Depok", "Tangerang", "Bekasi", "Yogyakarta", "Denpasar",
]

CATEGORY_PRICE_RANGES = {
    "Electronics": (150_000, 12_000_000),
    "Apparel": (50_000, 850_000),
    "Home & Kitchen": (40_000, 3_500_000),
    "Beauty": (25_000, 650_000),
    "Sports": (60_000, 2_200_000),
    "Books": (35_000, 300_000),
    "Toys": (45_000, 900_000),
    "Groceries": (10_000, 250_000),
}

MARKETING_CHANNELS = ["Email", "Social Media", "Search Ads", "Affiliate", "Influencer", "SMS"]

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)


def _random_date(start: date, end: date) -> date:
    span = (end - start).days
    return start + timedelta(days=random.randint(0, span))


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    channel: str
    start_date: date
    end_date: date


def generate_customers() -> pd.DataFrame:
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        signup_date = _random_date(SIGNUP_WINDOW_START, SIGNUP_WINDOW_END)
        rows.append(
            {
                "customer_id": f"CUST{i:06d}",
                "name": fake.name(),
                "email": fake.unique.email(),
                "city": random.choice(CITIES),
                "signup_date": signup_date.isoformat(),
            }
        )
    return pd.DataFrame(rows)


def generate_products() -> pd.DataFrame:
    rows = []
    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choice(list(CATEGORY_PRICE_RANGES.keys()))
        low, high = CATEGORY_PRICE_RANGES[category]
        price = round(random.uniform(low, high), -2)
        rows.append(
            {
                "product_id": f"PROD{i:05d}",
                "product_name": f"{fake.word().capitalize()} {category.split(' ')[0]} {fake.word().capitalize()}",
                "category": category,
                "price": price,
            }
        )
    return pd.DataFrame(rows)


def generate_campaigns() -> pd.DataFrame:
    total_days = (TRANSACTION_WINDOW_END - SIGNUP_WINDOW_START).days
    slot_size = total_days // NUM_CAMPAIGNS

    campaigns = []
    for i in range(1, NUM_CAMPAIGNS + 1):
        slot_start = SIGNUP_WINDOW_START + timedelta(days=(i - 1) * slot_size)
        start_date = slot_start + timedelta(days=random.randint(0, max(slot_size - 45, 1)))
        end_date = start_date + timedelta(days=random.randint(14, 45))
        channel = random.choice(MARKETING_CHANNELS)
        campaigns.append(
            Campaign(campaign_id=f"CAMP{i:04d}", channel=channel, start_date=start_date, end_date=end_date)
        )

    rows = [
        {
            "campaign_id": c.campaign_id,
            "campaign_name": f"{c.channel} - {c.start_date.strftime('%b %Y')} Push",
            "start_date": c.start_date.isoformat(),
            "end_date": c.end_date.isoformat(),
            "channel": c.channel,
        }
        for c in campaigns
    ]
    return pd.DataFrame(rows), campaigns


def _campaign_for_date(campaigns: list[Campaign], on_date: date) -> str | None:
    active = [c for c in campaigns if c.start_date <= on_date <= c.end_date]
    if not active or random.random() > 0.4:
        return None
    return random.choice(active).campaign_id


def generate_transactions_and_items(
    customers: pd.DataFrame, products: pd.DataFrame, campaigns: list[Campaign]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    customer_signup = dict(zip(customers["customer_id"], pd.to_datetime(customers["signup_date"]).dt.date))
    product_ids = products["product_id"].tolist()
    product_price = dict(zip(products["product_id"], products["price"]))

    transactions, items = [], []
    item_seq = 1

    for i in range(1, NUM_TRANSACTIONS + 1):
        customer_id = random.choice(customers["customer_id"].tolist())
        earliest = customer_signup[customer_id]
        if earliest >= TRANSACTION_WINDOW_END:
            continue
        transaction_date = _random_date(earliest, TRANSACTION_WINDOW_END)
        transaction_id = f"TXN{i:07d}"

        num_items = random.randint(1, MAX_ITEMS_PER_TRANSACTION)
        chosen_products = random.sample(product_ids, k=min(num_items, len(product_ids)))
        line_total = 0.0
        for product_id in chosen_products:
            quantity = random.randint(1, 4)
            unit_price = product_price[product_id]
            line_total += quantity * unit_price
            items.append(
                {
                    "transaction_item_id": f"TI{item_seq:08d}",
                    "transaction_id": transaction_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "price": unit_price,
                }
            )
            item_seq += 1

        transactions.append(
            {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "transaction_date": transaction_date.isoformat(),
                "total_amount": round(line_total, 2),
                "campaign_id": _campaign_for_date(campaigns, transaction_date),
            }
        )

    return pd.DataFrame(transactions), pd.DataFrame(items)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    customers = generate_customers()
    products = generate_products()
    campaigns_df, campaigns = generate_campaigns()
    transactions, transaction_items = generate_transactions_and_items(customers, products, campaigns)

    datasets = {
        "customers.csv": customers,
        "products.csv": products,
        "marketing_campaigns.csv": campaigns_df,
        "transactions.csv": transactions,
        "transaction_items.csv": transaction_items,
    }

    for filename, frame in datasets.items():
        frame.to_csv(OUTPUT_DIR / filename, index=False)
        print(f"{filename:<28} {len(frame):>6} rows")


if __name__ == "__main__":
    main()
