from aiochlite.converters._type_parsing import split_type_arguments


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
