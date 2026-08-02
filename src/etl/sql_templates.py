"""Minimal renderer for the `{{ params.key }}` placeholders used across sql/.

The same .sql files are rendered by Airflow's Jinja engine when run through a
DAG and by this lightweight substitution when run standalone (see
bootstrap_historical_load.py). It only needs to understand the one
placeholder shape actually used in this project, so a full template engine
would be unnecessary weight for a script that has no other Jinja needs.
"""

from __future__ import annotations

import re
from pathlib import Path

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"

_PLACEHOLDER = re.compile(r"\{\{\s*params\.(\w+)\s*\}\}")


def render_sql(relative_path: str, **params: str) -> str:
    text = (SQL_ROOT / relative_path).read_text()

    def substitute(match: re.Match) -> str:
        key = match.group(1)
        if key not in params:
            raise KeyError(f"missing value for placeholder 'params.{key}' in {relative_path}")
        return str(params[key])

    return _PLACEHOLDER.sub(substitute, text)
