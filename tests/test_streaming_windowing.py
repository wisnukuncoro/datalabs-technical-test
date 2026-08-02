from datetime import datetime, timedelta, timezone

from src.streaming.consumer import TumblingWindowAggregator, window_start_for
from src.streaming.sinks import CompositeSink


class RecordingSink:
    def __init__(self):
        self.flushed = []

    def emit(self, window):
        self.flushed.append((window.window_start, window.transaction_count, window.total_amount))


def test_window_start_for_floors_to_the_minute():
    event_time = datetime(2026, 1, 1, 12, 0, 47, tzinfo=timezone.utc)
    assert window_start_for(event_time) == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_aggregator_flushes_only_after_watermark_passes_window_end():
    sink = RecordingSink()
    aggregator = TumblingWindowAggregator(CompositeSink([sink]))
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    aggregator.process(base, 100.0)
    aggregator.process(base + timedelta(seconds=30), 50.0)
    assert sink.flushed == []

    aggregator.process(base + timedelta(seconds=65), 10.0)
    assert sink.flushed == []

    aggregator.process(base + timedelta(seconds=75), 10.0)
    assert len(sink.flushed) == 1
    window_start, count, total = sink.flushed[0]
    assert window_start == base
    assert count == 2
    assert total == 150.0


def test_late_arrival_within_grace_period_still_counts_in_its_window():
    sink = RecordingSink()
    aggregator = TumblingWindowAggregator(CompositeSink([sink]))
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    aggregator.process(base + timedelta(seconds=58), 100.0)
    aggregator.process(base + timedelta(seconds=61), 5.0)
    aggregator.process(base + timedelta(seconds=55), 20.0)  # arrives late, still within the same window
    aggregator.process(base + timedelta(seconds=130), 1.0)  # forces the watermark past the first window

    first_window = next(f for f in sink.flushed if f[0] == base)
    assert first_window[1] == 2
    assert first_window[2] == 120.0
