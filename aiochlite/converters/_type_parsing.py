import re
from functools import lru_cache
from typing import Final
from zoneinfo import ZoneInfo

_TOP_LEVEL_COMMA_SPLIT_RE: Final[re.Pattern[str]] = re.compile(r",(?![^()]*\))")
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
    return [part.strip() for part in _TOP_LEVEL_COMMA_SPLIT_RE.split(type_list) if part.strip()]


@lru_cache(maxsize=256)
def extract_timezone(ch_type: str) -> ZoneInfo | None:
    match = _DATETIME_TZ_RE.search(unwrap_wrappers(ch_type))
    if not match:
        return None

    tz = match.group(1)
    try:
        return ZoneInfo(tz)
    except Exception:
        return None
