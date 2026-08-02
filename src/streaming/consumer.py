"""Aggregates the dummy transaction stream into one-minute tumbling windows.

Windowing is done on event time (the timestamp the producer attached to each
message), not on Kafka ingestion time, so the counts are correct even if
messages arrive slightly out of order. A window is only flushed once the
watermark - the latest event time seen, minus an allowed-lateness grace
period - has moved past its end, giving late arrivals a chance to land in the
right bucket before the window closes.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer

from src.streaming.sinks import BigQuerySink, CompositeSink, ConsoleSink

TOPIC = "transactions.raw"
WINDOW_SIZE = timedelta(minutes=1)
ALLOWED_LATENESS = timedelta(seconds=10)


@dataclass
class WindowAggregate:
    window_start: datetime
    transaction_count: int = 0
    total_amount: float = 0.0

    def add(self, amount: float) -> None:
        self.transaction_count += 1
        self.total_amount += amount


def window_start_for(event_time: datetime) -> datetime:
    window_seconds = int(WINDOW_SIZE.total_seconds())
    epoch_seconds = int(event_time.timestamp())
    floored = epoch_seconds - (epoch_seconds % window_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


class TumblingWindowAggregator:
    def __init__(self, sink: CompositeSink) -> None:
        self._sink = sink
        self._windows: dict[datetime, WindowAggregate] = {}
        self._watermark = datetime.min.replace(tzinfo=timezone.utc)

    def process(self, event_time: datetime, amount: float) -> None:
        window_start = window_start_for(event_time)
        self._windows.setdefault(window_start, WindowAggregate(window_start)).add(amount)
        self._watermark = max(self._watermark, event_time - ALLOWED_LATENESS)
        self._flush_closed_windows()

    def _flush_closed_windows(self) -> None:
        closed_starts = sorted(start for start in self._windows if start + WINDOW_SIZE <= self._watermark)
        for start in closed_starts:
            self._sink.emit(self._windows.pop(start))


def build_consumer(bootstrap_servers: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="latest",
    )


def run(consumer: KafkaConsumer, aggregator: TumblingWindowAggregator) -> None:
    for message in consumer:
        payload = message.value
        event_time = datetime.fromisoformat(payload["event_time"])
        aggregator.process(event_time, payload["amount"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Per-minute transaction count aggregator")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group-id", default="transaction-window-aggregator")
    parser.add_argument("--bq-project-id", default=None, help="enables the BigQuery sink when set")
    parser.add_argument("--bq-table", default=None, help="e.g. my-project.streaming.transaction_window_counts")
    args = parser.parse_args()

    sink = CompositeSink([ConsoleSink()])
    if args.bq_project_id and args.bq_table:
        sink.add(BigQuerySink(args.bq_project_id, args.bq_table))

    consumer = build_consumer(args.bootstrap_servers, args.group_id)
    aggregator = TumblingWindowAggregator(sink)
    run(consumer, aggregator)


if __name__ == "__main__":
    main()
