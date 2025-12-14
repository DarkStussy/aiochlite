import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final
from uuid import UUID
from zoneinfo import ZoneInfo

_TOP_LEVEL_COMMA_SPLIT_RE: Final[re.Pattern] = re.compile(r",(?![^()]*\))")
_DATETIME_TZ_RE: Final[re.Pattern] = re.compile(r"DateTime(?:64)?\(\s*(?:\d+\s*,\s*)?'([^']+)'\s*\)", re.IGNORECASE)


def _parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_uuid(value: str) -> UUID:
    return UUID(value)


def _parse_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value

    return Decimal(value)


def _extract_base_type(ch_type: str) -> str:
    """Extract base type from ClickHouse type."""
    if ch_type.startswith("Nullable("):
        inner = ch_type[9:-1]
        return _extract_base_type(inner)

    if ch_type.startswith("LowCardinality("):
        inner = ch_type[15:-1]
        return _extract_base_type(inner)

    if "(" in ch_type:
        return ch_type[: ch_type.index("(")]

    return ch_type


def _unwrap_wrappers(ch_type: str) -> str:
    """Remove Nullable/LowCardinality wrappers while keeping inner type definition intact."""
    unwrapped = ch_type.strip()
    while True:
        if unwrapped.startswith("Nullable(") and unwrapped.endswith(")"):
            unwrapped = unwrapped[9:-1].strip()
            continue
        if unwrapped.startswith("LowCardinality(") and unwrapped.endswith(")"):
            unwrapped = unwrapped[15:-1].strip()
            continue
        return unwrapped


def _split_type_arguments(type_list: str) -> list[str]:
    """Split type arguments using regex that ignores commas inside parentheses."""
    return [part.strip() for part in _TOP_LEVEL_COMMA_SPLIT_RE.split(type_list) if part.strip()]


def _parse_tuple(value: list, ch_type: str) -> tuple:
    """Parse ClickHouse Tuple to Python tuple with element conversion."""
    tuple_type = _unwrap_wrappers(ch_type)
    if not tuple_type.startswith("Tuple("):
        return tuple(value)

    # Extract inner types from Tuple(Type1, Type2, ...)
    inner = tuple_type[6:-1]
    element_types = _split_type_arguments(inner)

    converted = []
    for val, elem_type in zip(value, element_types, strict=False):
        converted.append(from_clickhouse(val, elem_type))

    return tuple(converted)


def _parse_array(value: list, ch_type: str) -> list:
    """Parse ClickHouse Array with element conversion."""
    if not ch_type.startswith("Array("):
        return value

    # Extract inner type from Array(Type)
    inner = ch_type[6:-1]

    return [from_clickhouse(item, inner) for item in value]


def _extract_timezone(ch_type: str) -> ZoneInfo | None:
    """Extract timezone from DateTime/DateTime64 type definition."""
    match = _DATETIME_TZ_RE.search(_unwrap_wrappers(ch_type))
    if not match:
        return None

    tz = match.group(1)
    try:
        return ZoneInfo(tz)
    except Exception:
        return None


def _parse_string_type(value: str, base_type: str, ch_type: str) -> Any:
    """Parse string-based ClickHouse types."""
    tz = None
    if base_type in {"DateTime", "DateTime64"}:
        tz = _extract_timezone(ch_type)

    if base_type == "DateTime":
        dt = _parse_datetime(value)
        return dt.replace(tzinfo=tz) if tz else dt
    if base_type == "DateTime64":
        dt = datetime.fromisoformat(value)
        return dt.replace(tzinfo=tz) if tz else dt
    if base_type == "Date":
        return _parse_date(value)
    if base_type == "UUID":
        return _parse_uuid(value)

    return value


def from_clickhouse(value: Any, ch_type: str) -> Any:
    """
    Convert value from ClickHouse type to Python type.

    Args:
        value: Value from ClickHouse response.
        ch_type: ClickHouse type name (e.g., 'DateTime', 'Nullable(UUID)').

    Returns:
        Any: Converted Python value.
    """
    if value is None:
        return None

    base_type = _extract_base_type(ch_type)

    if base_type.startswith("Decimal"):
        return _parse_decimal(value)

    if isinstance(value, str):
        return _parse_string_type(value, base_type, ch_type)

    if isinstance(value, list):
        if base_type == "Tuple":
            return _parse_tuple(value, ch_type)
        if base_type == "Array":
            return _parse_array(value, ch_type)

    return value
