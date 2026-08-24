import pytest

from aiochlite.converters._type_parsing import (
    extract_base_type,
    parse_timezone,
    simple_aggregate_element,
    split_field_name,
    split_tuple_elements,
    split_type_arguments,
    to_binary_type,
    unwrap_wrappers,
)
from aiochlite.exceptions import ChProtocolError


def test_split_type_arguments_nested_parentheses() -> None:
    args = "String, Array(Tuple(Date, Int32, Int32, Decimal(9, 2)))"
    assert split_type_arguments(args) == [
        "String",
        "Array(Tuple(Date, Int32, Int32, Decimal(9, 2)))",
    ]


def test_split_type_arguments_tuple_elements() -> None:
    args = "Date, Int32, Int32, Decimal(9, 2)"
    assert split_type_arguments(args) == [
        "Date",
        "Int32",
        "Int32",
        "Decimal(9, 2)",
    ]


def test_split_type_arguments_with_timezone_quotes() -> None:
    args = "DateTime64(6, 'Europe/Moscow'), Nullable(String)"
    assert split_type_arguments(args) == ["DateTime64(6, 'Europe/Moscow')", "Nullable(String)"]


def test_split_type_arguments_keeps_backquoted_names_whole():
    """A backquoted field name may hold the separators the scan stops at."""
    args = "`has space, comma` UInt8, `Enum8('a'=1)` String, `b\\`q` Int32"
    assert split_type_arguments(args) == [
        "`has space, comma` UInt8",
        "`Enum8('a'=1)` String",
        "`b\\`q` Int32",
    ]


def test_split_type_arguments_keeps_escaped_enum_label_whole():
    args = "Enum8('a\\'b, c' = 1, 'd' = 2), UInt8"
    assert split_type_arguments(args) == ["Enum8('a\\'b, c' = 1, 'd' = 2)", "UInt8"]


@pytest.mark.parametrize(
    ("element", "expected"),
    [
        ("UInt8", (None, "UInt8")),
        ("a UInt8", ("a", "UInt8")),
        ("Decimal(10, 2)", (None, "Decimal(10, 2)")),
        ("DateTime64(3, 'UTC')", (None, "DateTime64(3, 'UTC')")),
        ("d DateTime64(3, 'UTC')", ("d", "DateTime64(3, 'UTC')")),
        ("`has space, comma` UInt8", ("`has space, comma`", "UInt8")),
        ("`b\\`q` Int32", ("`b\\`q`", "Int32")),
        # No type behind it, so there is nothing to split off.
        ("`unterminated", (None, "`unterminated")),
    ],
)
def test_split_field_name(element: str, expected: tuple[str | None, str]):
    assert split_field_name(element) == expected


def test_split_tuple_elements_drops_names():
    assert split_tuple_elements("a UInt8, b String") == ["UInt8", "String"]
    assert split_tuple_elements("UInt8, String") == ["UInt8", "String"]


def test_simple_aggregate_element_is_the_wire_type():
    assert simple_aggregate_element("SimpleAggregateFunction(sum, UInt64)") == "UInt64"
    assert simple_aggregate_element("SimpleAggregateFunction(anyLast, Nullable(UInt64))") == "Nullable(UInt64)"
    assert simple_aggregate_element("SimpleAggregateFunction(maxMap, Map(UInt8, UInt8))") == "Map(UInt8, UInt8)"


def test_simple_aggregate_element_rejects_a_malformed_definition():
    with pytest.raises(ValueError, match="Invalid SimpleAggregateFunction"):
        simple_aggregate_element("SimpleAggregateFunction(sum)")


def test_simple_aggregate_function_is_transparent():
    assert unwrap_wrappers("SimpleAggregateFunction(sum, UInt64)") == "UInt64"
    assert extract_base_type("SimpleAggregateFunction(sum, UInt64)") == "UInt64"
    assert extract_base_type("SimpleAggregateFunction(maxMap, Map(UInt8, UInt8))") == "Map"


def test_to_binary_type_keeps_tuple_field_names():
    assert to_binary_type("Tuple(a UInt8, b String)") == "Tuple(a UInt8, b Binary)"
    assert to_binary_type("Tuple(`odd name` String)") == "Tuple(`odd name` Binary)"


def test_to_binary_type_reaches_through_simple_aggregate_function():
    assert to_binary_type("SimpleAggregateFunction(min, String)") == "SimpleAggregateFunction(min, Binary)"


def test_to_binary_type_leaves_a_textless_named_tuple_alone():
    """`holds_text` compares against the input, so a no-op rewrite has to reproduce it exactly."""
    for ch_type in ("Tuple(a UInt8, b Decimal(10, 2))", "Tuple(`odd name` UInt8)"):
        assert to_binary_type(ch_type) == ch_type


def test_parse_timezone_without_a_name():
    assert parse_timezone(None) is None
    assert parse_timezone("") is None


def test_unloadable_timezone_raises():
    """Returning None here would decode every DateTime in the local zone, hours off and silently."""
    with pytest.raises(ChProtocolError, match="Not/AZone"):
        parse_timezone("Not/AZone")
