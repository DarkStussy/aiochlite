import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from .to_clickhouse import format_timedelta


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, timedelta):
        return format_timedelta(value)
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def to_json(data: Any) -> str:
    """Convert Python data to JSON string for ClickHouse HTTP API."""
    return json.dumps(data, default=_json_default, ensure_ascii=False, separators=(",", ":"))
