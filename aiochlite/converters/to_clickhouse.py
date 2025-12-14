from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


def _format_collection(value: tuple | list | dict) -> str:
    """Format collection types to ClickHouse format."""
    if isinstance(value, tuple):
        items = ",".join(_to_clickhouse_format(item) for item in value)
        return f"({items})"
    if isinstance(value, list):
        items = ",".join(_to_clickhouse_format(item) for item in value)
        return f"[{items}]"

    items = ",".join(f"'{k}':{_to_clickhouse_format(v)}" for k, v in value.items())
    return f"{{{items}}}"


def _to_clickhouse_format(value: Any) -> str:
    """Convert value to ClickHouse format with single quotes for strings."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, (tuple, list, dict)):
        return _format_collection(value)

    return str(value)


def _prepare_for_json(value: Any) -> Any:
    """Prepare Python value for JSON serialization by converting special types."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, tuple):
        return tuple(_prepare_for_json(item) for item in value)

    if isinstance(value, list):
        return [_prepare_for_json(item) for item in value]

    if isinstance(value, dict):
        return {k: _prepare_for_json(v) for k, v in value.items()}

    return _convert_special_type(value)


def _convert_special_type(value: Any) -> str:
    """Convert special types to string representation."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8")

    return str(value)


def to_clickhouse(value: Any) -> str | int | float:
    """
    Convert Python value to ClickHouse parameter format.

    Args:
        value (Any): Python value to convert.

    Returns:
        str | int | float: Converted value suitable for ClickHouse.
    """
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return 1 if value else 0

    if isinstance(value, (int, float, str)):
        return value

    if isinstance(value, (list, tuple, dict)):
        prepared = _prepare_for_json(value)
        return _to_clickhouse_format(prepared)

    return _convert_special_type(value)
