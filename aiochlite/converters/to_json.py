import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


def _convert_special_type(value: Any) -> str:
    """Convert special types to string representation."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _convert_value(value: Any) -> Any:
    """Convert Python value to JSON-serializable format."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, (datetime, date, UUID, Decimal, bytes)):
        return _convert_special_type(value)

    if isinstance(value, tuple):
        return tuple(_convert_value(item) for item in value)
    if isinstance(value, list):
        return [_convert_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}

    return str(value)


def to_json(data: Any) -> str:
    """Convert Python data to JSON string for ClickHouse HTTP API."""
    converted = _convert_value(data)
    return json.dumps(converted, ensure_ascii=False, separators=(",", ":"))
