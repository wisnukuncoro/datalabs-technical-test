"""Output destinations for windowed transaction aggregates.

Console output is always active so the pipeline is demonstrable with no
external dependencies. BigQuery is added on top when a project and table are
supplied, via streaming inserts into a small, its-own partitioned table.
"""

from __future__ import annotations

from typing import Protocol

from google.cloud import bigquery


class WindowAggregate(Protocol):
    window_start: object
    transaction_count: int
    total_amount: float


class WindowSink(Protocol):
    def emit(self, window: WindowAggregate) -> None: ...


class ConsoleSink:
    def emit(self, window: WindowAggregate) -> None:
        print(
            f"[{window.window_start.isoformat()}] "
            f"transactions={window.transaction_count} total_amount={window.total_amount:,.2f}"
        )


class BigQuerySink:
    def __init__(self, project_id: str, table: str) -> None:
        self._client = bigquery.Client(project=project_id)
        self._table = table

    def emit(self, window: WindowAggregate) -> None:
        errors = self._client.insert_rows_json(
            self._table,
            [
                {
                    "window_start": window.window_start.isoformat(),
                    "transaction_count": window.transaction_count,
                    "total_amount": window.total_amount,
                }
            ],
        )
        if errors:
            raise RuntimeError(f"failed to insert window aggregate: {errors}")


class CompositeSink:
    def __init__(self, sinks: list[WindowSink]) -> None:
        self._sinks = list(sinks)

    def add(self, sink: WindowSink) -> None:
        self._sinks.append(sink)

    def emit(self, window: WindowAggregate) -> None:
        for sink in self._sinks:
            sink.emit(window)
