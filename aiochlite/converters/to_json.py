import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from .to_clickhouse import format_datetime, format_timedelta


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        out = value.value
    elif isinstance(value, date):
        out = format_datetime(value) if isinstance(value, datetime) else value.strftime("%Y-%m-%d")
    elif isinstance(value, timedelta):
        out = format_timedelta(value)
    elif isinstance(value, (UUID, Decimal)):
        out = str(value)
    elif isinstance(value, (set, frozenset)):
        out = list(value)
    elif isinstance(value, bytes):
        # The surrogates become those bytes again when the body is encoded.
        out = value.decode("utf-8", "surrogateescape")
    else:
        out = str(value)

    return out


def _is_json_key(key: Any) -> bool:
    return isinstance(key, (str, int, float)) or key is None


def _json_key(key: Any) -> Any:
    """A key `dumps` accepts, rendered by the rules `_json_default` gives values."""
    if _is_json_key(key):
        return key

    rendered = _json_default(key)
    # An Enum yields its value, which may itself need rendering.
    if not _is_json_key(rendered):
        rendered = _json_default(rendered)
    # A set yields a list, which no key can be.
    return rendered if _is_json_key(rendered) else str(rendered)


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
