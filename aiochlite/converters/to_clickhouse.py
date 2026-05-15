from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID


def format_timedelta(td: timedelta) -> str:
    total_us = td.days * 86_400_000_000 + td.seconds * 1_000_000 + td.microseconds
    sign = "-" if total_us < 0 else ""
    total_us = abs(total_us)
    total_seconds, micros = divmod(total_us, 1_000_000)
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if micros:
        return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}.{micros:06d}"

    return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"


class _MissingType:
    __slots__ = ()


_MISSING = _MissingType()


def _escape_ch_string_literal(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
        .replace("\0", "\\0")
        .replace("'", "\\'")
    )


def _scalar_clickhouse_literal(value: Any) -> str | _MissingType:
    if value is None:
        out: str | _MissingType = "NULL"
    else:
        value_type = type(value)
        if value_type is bool:
            out = "true" if value else "false"
        elif value_type is int or value_type is float:
            out = str(value)
        elif value_type is str:
            out = f"'{_escape_ch_string_literal(value)}'"
        elif isinstance(value, datetime):
            out = f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, date):
            out = f"'{value.strftime('%Y-%m-%d')}'"
        elif isinstance(value, timedelta):
            out = f"'{format_timedelta(value)}'"
        elif isinstance(value, (UUID, Decimal)):
            out = f"'{value}'"
        elif isinstance(value, bytes):
            out = f"'{_escape_ch_string_literal(value.decode('utf-8'))}'"
        else:
            out = _MISSING

    return out


def _container_clickhouse_literal(value: Any) -> str | _MissingType:
    if isinstance(value, tuple):
        return f"({','.join(_to_clickhouse_literal(item) for item in value)})"
    if isinstance(value, list):
        return f"[{','.join(_to_clickhouse_literal(item) for item in value)}]"
    if isinstance(value, dict):
        items = ",".join(f"{_to_clickhouse_literal(str(k))}:{_to_clickhouse_literal(v)}" for k, v in value.items())
        return f"{{{items}}}"

    return _MISSING


def _to_clickhouse_literal(value: Any) -> str:
    """Render Python value as a ClickHouse literal (used for container params)."""
    scalar = _scalar_clickhouse_literal(value)
    if not isinstance(scalar, _MissingType):
        return scalar

    container = _container_clickhouse_literal(value)
    if not isinstance(container, _MissingType):
        return container

    return f"'{_escape_ch_string_literal(str(value))}'"


def to_clickhouse(value: Any) -> str | int | float:
    """
    Convert Python value to ClickHouse parameter format.

    Args:
        value (Any): Python value to convert.

    Returns:
        str | int | float: Converted value suitable for ClickHouse.
    """
    if value is None:
        out: str | int | float = "\\N"
    else:
        value_type = type(value)
        if value_type is bool:
            out = 1 if value else 0
        elif value_type is int or value_type is float or value_type is str:
            out = value
        elif isinstance(value, (list, tuple, dict)):
            out = _to_clickhouse_literal(value)
        elif isinstance(value, datetime):
            out = value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, date):
            out = value.strftime("%Y-%m-%d")
        elif isinstance(value, timedelta):
            out = format_timedelta(value)
        elif isinstance(value, (UUID, Decimal)):
            out = str(value)
        elif isinstance(value, bytes):
            out = value.decode("utf-8")
        else:
            out = str(value)

    return out
