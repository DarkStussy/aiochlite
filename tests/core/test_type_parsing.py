import pytest

from aiochlite.converters._type_parsing import parse_timezone, split_type_arguments
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


def test_parse_timezone_without_a_name():
    assert parse_timezone(None) is None
    assert parse_timezone("") is None


def test_unloadable_timezone_raises():
    """Returning None here would decode every DateTime in the local zone, hours off and silently."""
    with pytest.raises(ChProtocolError, match="Not/AZone"):
        parse_timezone("Not/AZone")
