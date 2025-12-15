import re
from functools import lru_cache
from typing import Final
from zoneinfo import ZoneInfo

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
