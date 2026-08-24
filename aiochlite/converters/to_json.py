import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from .to_clickhouse import format_datetime, format_timedelta


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, date):
        return format_datetime(value) if isinstance(value, datetime) else value.strftime("%Y-%m-%d")
    if isinstance(value, timedelta):
        return format_timedelta(value)
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _json_key(key: Any) -> Any:
    """A key `dumps` accepts, rendered by the rules `_json_default` gives values."""
    if isinstance(key, (str, int, float)) or key is None:
        return key

    rendered = _json_default(key)
    # An Enum yields its value, which may itself need rendering. Nothing needs a third pass.
    return rendered if isinstance(rendered, (str, int, float)) or rendered is None else _json_default(rendered)


def _with_rendered_keys(value: Any) -> Any:
    if isinstance(value, dict):
        return {_json_key(key): _with_rendered_keys(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_with_rendered_keys(item) for item in value]

    return value


def to_json(data: Any) -> str:
    """Convert Python data to JSON string for ClickHouse HTTP API."""
    try:
        return json.dumps(data, default=_json_default, ensure_ascii=False, separators=(",", ":"))
    except TypeError:
        # Rendering keys upfront costs every row 33%-90%, so the walk waits for one to fail.
        rendered = _with_rendered_keys(data)

    return json.dumps(rendered, default=_json_default, ensure_ascii=False, separators=(",", ":"))
