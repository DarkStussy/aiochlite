import re
from functools import lru_cache
from typing import Final
from zoneinfo import ZoneInfo

from aiochlite.exceptions import ChProtocolError

_DATETIME_TZ_RE: Final[re.Pattern[str]] = re.compile(
    r"DateTime(?:64)?\(\s*(?:\d+\s*,\s*)?'([^']+)'\s*\)",
    re.IGNORECASE,
)


@lru_cache(maxsize=256)
def extract_base_type(ch_type: str) -> str:
    if ch_type.startswith("Nullable("):
        return extract_base_type(ch_type[9:-1])

    if ch_type.startswith("LowCardinality("):
        return extract_base_type(ch_type[15:-1])

    if "(" in ch_type:
        return ch_type[: ch_type.index("(")]

    return ch_type


@lru_cache(maxsize=256)
def unwrap_wrappers(ch_type: str) -> str:
    unwrapped = ch_type.strip()
    while True:
        if unwrapped.startswith("Nullable(") and unwrapped.endswith(")"):
            unwrapped = unwrapped[9:-1].strip()
            continue
        if unwrapped.startswith("LowCardinality(") and unwrapped.endswith(")"):
            unwrapped = unwrapped[15:-1].strip()
            continue
        return unwrapped


@lru_cache(maxsize=256)
def split_type_arguments(type_list: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_quote = False

    def _flush() -> None:
        part = "".join(buf).strip()
        if part:
            parts.append(part)
        buf.clear()

    for ch in type_list:
        if in_quote:
            buf.append(ch)
            if ch == "'":
                in_quote = False
            continue

        if ch == "'":
            in_quote = True
            buf.append(ch)
            continue

        if ch == "(":
            depth += 1
            buf.append(ch)
            continue

        if ch == ")":
            depth -= 1
            buf.append(ch)
            continue

        if ch == "," and depth == 0:
            _flush()
            continue

        buf.append(ch)

    _flush()

    return parts


# Types no server ever names: they mark a String/FixedString to decode as bytes.
BINARY_TYPE: Final[str] = "Binary"
FIXED_BINARY_TYPE: Final[str] = "FixedBinary"

_BINARY_WRAPPERS: Final[tuple[str, ...]] = ("Nullable(", "LowCardinality(", "Array(")
_BINARY_CONTAINERS: Final[tuple[str, ...]] = ("Map(", "Tuple(")


@lru_cache(maxsize=256)
def to_binary_type(ch_type: str) -> str:
    """Rewrite every nested ``String``/``FixedString`` to its binary pseudo-type.

    Rewriting the type instead of passing a flag keeps every decoder and cache keyed by type
    string alone.

    Args:
        ch_type (str): ClickHouse type.

    Returns:
        str: Rewritten type, or `ch_type` when it holds no text.
    """
    unwrapped = ch_type.strip()
    if unwrapped == "String":
        return BINARY_TYPE

    if unwrapped.startswith("FixedString(") and unwrapped.endswith(")"):
        return f"{FIXED_BINARY_TYPE}({unwrapped[12:-1].strip()})"

    for prefix in _BINARY_WRAPPERS:
        if unwrapped.startswith(prefix) and unwrapped.endswith(")"):
            return f"{prefix}{to_binary_type(unwrapped[len(prefix) : -1])})"

    # Split rather than scanned: an enum label can quote anything, type names included.
    for prefix in _BINARY_CONTAINERS:
        if unwrapped.startswith(prefix) and unwrapped.endswith(")"):
            arguments = split_type_arguments(unwrapped[len(prefix) : -1])
            return f"{prefix}{', '.join(to_binary_type(argument) for argument in arguments)})"

    return unwrapped


def holds_text(ch_type: str) -> bool:
    """Whether the type has a ``String``/``FixedString`` anywhere in it.

    Args:
        ch_type (str): ClickHouse type.

    Returns:
        bool: True if any part of it decodes as text.
    """
    return to_binary_type(ch_type) != ch_type.strip()


@lru_cache(maxsize=64)
def parse_timezone(name: str | None) -> ZoneInfo | None:
    if not name:
        return None

    try:
        return ZoneInfo(name)
    except Exception as error:
        # Falling back to None would decode DateTime in the local timezone and return it naive.
        raise ChProtocolError(f"Cannot load timezone {name!r}. Install `tzdata` if the system has none.") from error


@lru_cache(maxsize=256)
def extract_timezone(ch_type: str) -> ZoneInfo | None:
    match = _DATETIME_TZ_RE.search(unwrap_wrappers(ch_type))
    if not match:
        return None

    return parse_timezone(match.group(1))
