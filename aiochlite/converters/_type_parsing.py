import re
from functools import lru_cache
from typing import Final
from zoneinfo import ZoneInfo

from aiochlite.exceptions import ChProtocolError

_DATETIME_TZ_RE: Final[re.Pattern[str]] = re.compile(
    r"DateTime(?:64)?\(\s*(?:\d+\s*,\s*)?'([^']+)'\s*\)",
    re.IGNORECASE,
)


SIMPLE_AGGREGATE_PREFIX: Final[str] = "SimpleAggregateFunction("


def simple_aggregate_element(ch_type: str) -> str:
    """The `T` of `SimpleAggregateFunction(func, T)`: what the wire carries."""
    arguments = split_type_arguments(ch_type[len(SIMPLE_AGGREGATE_PREFIX) : -1])
    if len(arguments) != 2:
        raise ValueError(f"Invalid SimpleAggregateFunction definition: {ch_type}")

    return arguments[1]


@lru_cache(maxsize=256)
def extract_base_type(ch_type: str) -> str:
    if ch_type.startswith("Nullable("):
        return extract_base_type(ch_type[9:-1])

    if ch_type.startswith("LowCardinality("):
        return extract_base_type(ch_type[15:-1])

    if ch_type.startswith(SIMPLE_AGGREGATE_PREFIX):
        return extract_base_type(simple_aggregate_element(ch_type))

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
        if unwrapped.startswith(SIMPLE_AGGREGATE_PREFIX) and unwrapped.endswith(")"):
            unwrapped = simple_aggregate_element(unwrapped).strip()
            continue
        return unwrapped


def _quoted_run_end(text: str, start: int) -> int:
    """Index just past the quoted run opening at `start`, or the end if it never closes."""
    quote = text[start]
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "\\":  # `b\`q` closes at the second backquote, not the first
            index += 2
            continue
        if char == quote:
            return index + 1

        index += 1

    return len(text)


@lru_cache(maxsize=256)
def split_type_arguments(type_list: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    index = 0

    while index < len(type_list):
        char = type_list[index]
        # Enum labels are single-quoted, field names backquoted, and both may hold a comma.
        if char in "'`":
            index = _quoted_run_end(type_list, index)
            continue

        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            part = type_list[start:index].strip()
            if part:
                parts.append(part)
            start = index + 1

        index += 1

    last = type_list[start:].strip()
    if last:
        parts.append(last)

    return parts


_FIELD_NAME_RE: Final[re.Pattern[str]] = re.compile(r"[A-Za-z_][0-9A-Za-z_]*\s")


def split_field_name(element: str) -> tuple[str | None, str]:
    """A `Tuple` element split into its field name, verbatim, and its type.

    Only a field name puts whitespace at this level: a parameterized type keeps its arguments
    inside parentheses.
    """
    element = element.strip()
    if element.startswith("`"):
        end = _quoted_run_end(element, 0)
        name, rest = element[:end], element[end:].strip()
        return (name, rest) if rest else (None, element)  # unterminated: no type behind it

    match = _FIELD_NAME_RE.match(element)
    return (match.group().strip(), element[match.end() :].strip()) if match else (None, element)


@lru_cache(maxsize=256)
def split_tuple_elements(type_list: str) -> list[str]:
    """The element types of a `Tuple` argument list, with any field names dropped."""
    return [split_field_name(element)[1] for element in split_type_arguments(type_list)]


# Types no server ever names: they mark a String/FixedString to decode as bytes.
BINARY_TYPE: Final[str] = "Binary"
FIXED_BINARY_TYPE: Final[str] = "FixedBinary"

_BINARY_WRAPPERS: Final[tuple[str, ...]] = ("Nullable(", "LowCardinality(", "Array(")
# `SimpleAggregateFunction` belongs here: its aggregate name matches no rule below, so rewriting
# the argument list leaves the name alone.
_BINARY_CONTAINERS: Final[tuple[str, ...]] = ("Map(", "Tuple(", SIMPLE_AGGREGATE_PREFIX)


def _to_binary_element(element: str) -> str:
    """Rewrite one container element, putting back the field name a named `Tuple` carries."""
    name, element_type = split_field_name(element)
    rewritten = to_binary_type(element_type)
    return rewritten if name is None else f"{name} {rewritten}"


@lru_cache(maxsize=256)
def to_binary_type(ch_type: str) -> str:
    """Rewrite every nested ``String``/``FixedString`` to its binary pseudo-type."""
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
            return f"{prefix}{', '.join(_to_binary_element(argument) for argument in arguments)})"

    return unwrapped


def holds_text(ch_type: str) -> bool:
    """Whether the type has a ``String``/``FixedString`` anywhere in it."""
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
