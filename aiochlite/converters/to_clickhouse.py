from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


def format_datetime(value: datetime) -> str:
    """
    Render a datetime for a ClickHouse parameter.

    Aware values become a Unix timestamp, which fixes the instant regardless of the column
    timezone. Naive values keep wall-clock text and are read in the column timezone. Both keep
    microseconds when present; ``DateTime`` columns reject them, since they cannot store them.

    Args:
        value (datetime): Value to render.

    Returns:
        str: Timestamp for aware values, ``YYYY-MM-DD hh:mm:ss[.ffffff]`` for naive ones.
    """
    # tzinfo alone is not the test: one whose utcoffset() is None still leaves the value naive.
    if value.tzinfo is not None and value.utcoffset() is not None:
        total_us = (value - _EPOCH) // _MICROSECOND
        sign = "-" if total_us < 0 else ""
        seconds, micros = divmod(abs(total_us), 1_000_000)
        return f"{sign}{seconds}.{micros:06d}" if micros else f"{sign}{seconds}"

    text = f"{value.year:04d}-{value.month:02d}-{value.day:02d} {value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    return f"{text}.{value.microsecond:06d}" if value.microsecond else text


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


def quote_identifier(name: str) -> str:
    """
    Quote a database, table or column name for use in a query.

    Args:
        name (str): Identifier to quote.

    Returns:
        str: Backquoted identifier.
    """
    escaped = name.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{escaped}`"


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
            out = f"'{format_datetime(value)}'"
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


def _enum_value(value: Any) -> Any:
    """An `Enum` member stands for its value; `str()` on one renders `Color.RED`."""
    return value.value if isinstance(value, Enum) else value


def _container_clickhouse_literal(value: Any) -> str | _MissingType:
    if isinstance(value, tuple):
        return f"({','.join(_to_clickhouse_literal(item) for item in value)})"
    if isinstance(value, list):
        return f"[{','.join(_to_clickhouse_literal(item) for item in value)}]"
    if isinstance(value, dict):
        items = ",".join(
            f"{_to_clickhouse_literal(str(_enum_value(k)))}:{_to_clickhouse_literal(v)}" for k, v in value.items()
        )
        return f"{{{items}}}"

    return _MISSING


def _to_clickhouse_literal(value: Any) -> str:
    """Render Python value as a ClickHouse literal (used for container params)."""
    value = _enum_value(value)
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
    value = _enum_value(value)
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
            out = format_datetime(value)
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
