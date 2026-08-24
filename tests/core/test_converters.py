import json
from datetime import UTC, date, datetime, timedelta, tzinfo
from decimal import Decimal
from enum import Enum, IntEnum, IntFlag, StrEnum
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite.converters import quote_identifier, to_clickhouse, to_json


class TestToClickHouse:
    """Tests for Python to ClickHouse type conversion."""

    def test_basic_types(self):
        """Test basic type conversions."""
        assert to_clickhouse(value=None) == "\\N"
        assert to_clickhouse(value=True) == 1
        assert to_clickhouse(value=False) == 0
        assert to_clickhouse(42) == 42
        assert to_clickhouse(3.14) == 3.14
        assert to_clickhouse("hello") == "hello"

    def test_collections(self):
        """Test list and tuple conversions."""
        assert to_clickhouse([1, 2, 3]) == "[1,2,3]"
        assert to_clickhouse((1, 2, 3)) == "(1,2,3)"
        assert to_clickhouse(["a", "b", "c"]) == "['a','b','c']"
        assert to_clickhouse(("a", "b")) == "('a','b')"
        assert to_clickhouse([1, "test", 3.14]) == "[1,'test',3.14]"

    def test_nested_collections(self):
        """Test nested collection conversions."""
        assert to_clickhouse([[1, 2], [3, 4]]) == "[[1,2],[3,4]]"
        assert to_clickhouse({"key": "value"}) == "{'key':'value'}"
        assert to_clickhouse({"nums": [1, 2, 3]}) == "{'nums':[1,2,3]}"

    def test_datetime_types(self):
        """Test datetime and date conversions."""
        dt = datetime(2025, 12, 14, 15, 30, 45)
        assert to_clickhouse(dt) == "2025-12-14 15:30:45"

        d = date(2025, 12, 14)
        assert to_clickhouse(d) == "2025-12-14"

    def test_naive_datetime_keeps_microseconds(self):
        """Naive values stay wall-clock text, with the fraction only when it is non-zero."""
        assert to_clickhouse(datetime(2025, 12, 14, 15, 30, 45, 123456)) == "2025-12-14 15:30:45.123456"
        assert to_clickhouse(datetime(2025, 12, 14, 15, 30, 45, 1)) == "2025-12-14 15:30:45.000001"
        assert to_clickhouse(datetime(2025, 12, 14, 15, 30, 45)) == "2025-12-14 15:30:45"

    def test_aware_datetime_becomes_timestamp(self):
        """Aware values become a Unix timestamp, so the instant survives any column timezone."""
        moscow = datetime(2024, 1, 1, 15, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        utc = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        assert to_clickhouse(moscow) == to_clickhouse(utc) == "1704110400"
        assert to_clickhouse(utc.replace(microsecond=123456)) == "1704110400.123456"

    def test_aware_datetime_before_epoch(self):
        """Timestamps stay negative and keep the fraction on the same side of the sign."""
        assert to_clickhouse(datetime(1950, 1, 1, tzinfo=UTC)) == "-631152000"
        assert to_clickhouse(datetime(1949, 12, 31, 23, 59, 59, 876544, tzinfo=UTC)) == "-631152000.123456"

    def test_tzinfo_without_offset_is_naive(self):
        """Python calls a value naive when utcoffset() is None, whatever tzinfo is set."""

        class _NoOffset(tzinfo):
            def utcoffset(self, dt: datetime | None) -> timedelta | None:
                return None

            def dst(self, dt: datetime | None) -> timedelta | None:
                return None

            def tzname(self, dt: datetime | None) -> str | None:
                return None

        value = datetime(2025, 12, 14, 15, 30, 45, tzinfo=_NoOffset())
        assert to_clickhouse(value) == "2025-12-14 15:30:45"

    def test_datetime_in_container_literal(self):
        """Container literals quote the same rendering."""
        assert to_clickhouse([datetime(2025, 12, 14, 15, 30, 45, 123456)]) == "['2025-12-14 15:30:45.123456']"
        assert to_clickhouse([datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)]) == "['1704110400']"

    def test_timedelta(self):
        """Test timedelta → Time/Time64 literal conversions."""
        assert to_clickhouse(timedelta(seconds=3661)) == "01:01:01"
        assert to_clickhouse(timedelta(seconds=-3661)) == "-01:01:01"
        assert to_clickhouse(timedelta(seconds=1, microseconds=500_000)) == "00:00:01.500000"
        assert to_clickhouse(timedelta(hours=100, minutes=30)) == "100:30:00"
        assert to_clickhouse([timedelta(seconds=60), timedelta(seconds=-60)]) == "['00:01:00','-00:01:00']"

    def test_special_types(self):
        """Test UUID and Decimal conversions."""
        uid = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert to_clickhouse(uid) == "550e8400-e29b-41d4-a716-446655440000"

        dec = Decimal("123.456")
        assert to_clickhouse(dec) == "123.456"

    def test_bytes(self):
        """Test bytes conversion."""
        assert to_clickhouse(b"hello") == "hello"


class TestToJson:
    """Tests for Python to JSON conversion for HTTP API."""

    def test_basic_dict(self):
        """Test basic dictionary conversion."""
        result = to_json({"id": 1, "name": "Alice"})
        assert result == '{"id":1,"name":"Alice"}'

    def test_basic_list(self):
        """Test basic list conversion."""
        result = to_json([1, 2, 3])
        assert result == "[1,2,3]"

    def test_datetime_in_dict(self):
        """Test datetime conversion in dictionary."""
        data = {"created_at": datetime(2025, 12, 14, 15, 30, 45)}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["created_at"] == "2025-12-14 15:30:45"

    def test_date_in_dict(self):
        """Test date conversion in dictionary."""
        data = {"birth_date": date(2025, 12, 14)}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["birth_date"] == "2025-12-14"

    def test_timedelta_in_dict(self):
        """Test timedelta → Time/Time64 string in JSON payload."""
        data = {
            "tod": timedelta(seconds=3661),
            "tod_neg": timedelta(seconds=-3661, microseconds=-500),
        }
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["tod"] == "01:01:01"
        assert parsed["tod_neg"] == "-01:01:01.000500"

    def test_uuid_in_dict(self):
        """Test UUID conversion in dictionary."""
        uid = UUID("550e8400-e29b-41d4-a716-446655440000")
        data = {"user_id": uid}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["user_id"] == "550e8400-e29b-41d4-a716-446655440000"

    def test_decimal_in_dict(self):
        """Test Decimal conversion in dictionary."""
        data = {"price": Decimal("123.456")}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["price"] == "123.456"

    def test_nested_structures(self):
        """Test nested structures with special types."""
        data = {
            "id": 1,
            "created_at": datetime(2025, 12, 14, 10, 0, 0),
            "tags": ["python", "clickhouse"],
            "metadata": {"version": "1.0", "active": True},
        }
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["id"] == 1
        assert parsed["created_at"] == "2025-12-14 10:00:00"
        assert parsed["tags"] == ["python", "clickhouse"]
        assert parsed["metadata"]["version"] == "1.0"
        assert parsed["metadata"]["active"] is True

    def test_list_of_dicts(self):
        """Test list of dictionaries with special types."""
        data = [
            {"id": 1, "created_at": datetime(2025, 12, 14, 10, 0, 0)},
            {"id": 2, "created_at": datetime(2025, 12, 14, 11, 0, 0)},
        ]
        result = to_json(data)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["created_at"] == "2025-12-14 10:00:00"
        assert parsed[1]["created_at"] == "2025-12-14 11:00:00"

    def test_bytes_conversion(self):
        """Test bytes conversion."""
        data = {"data": b"hello world"}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["data"] == "hello world"

    def test_none_values(self):
        """Test None values handling."""
        data = {"id": 1, "name": "Alice", "email": None}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["id"] == 1
        assert parsed["name"] == "Alice"
        assert parsed["email"] is None

    def test_tuple_preservation(self):
        """Test tuple is preserved in conversion."""
        data = {"coordinates": (10.5, 20.3, 30.1)}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["coordinates"] == [10.5, 20.3, 30.1]

    def test_empty_collections(self):
        """Test empty collections."""
        data = {"empty_list": [], "empty_dict": {}, "items": []}
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["empty_list"] == []
        assert parsed["empty_dict"] == {}
        assert parsed["items"] == []

    def test_mixed_types(self):
        """Test mixed types in single structure."""
        data = {
            "int": 42,
            "float": 3.14,
            "str": "text",
            "bool": True,
            "none": None,
            "datetime": datetime(2025, 12, 14, 15, 30, 45),
            "date": date(2025, 12, 14),
            "uuid": UUID("550e8400-e29b-41d4-a716-446655440000"),
            "decimal": Decimal("99.99"),
            "list": [1, 2, 3],
            "dict": {"key": "value"},
        }
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["int"] == 42
        assert parsed["float"] == 3.14
        assert parsed["str"] == "text"
        assert parsed["bool"] is True
        assert parsed["none"] is None
        assert parsed["datetime"] == "2025-12-14 15:30:45"
        assert parsed["date"] == "2025-12-14"
        assert parsed["uuid"] == "550e8400-e29b-41d4-a716-446655440000"
        assert parsed["decimal"] == "99.99"
        assert parsed["list"] == [1, 2, 3]
        assert parsed["dict"] == {"key": "value"}

    def test_nested_datetime(self):
        """Test datetime in nested structures."""
        data = {
            "events": [
                {"name": "Event 1", "timestamp": datetime(2025, 12, 14, 10, 0, 0)},
                {"name": "Event 2", "timestamp": datetime(2025, 12, 14, 11, 0, 0)},
            ]
        }
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed["events"][0]["timestamp"] == "2025-12-14 10:00:00"
        assert parsed["events"][1]["timestamp"] == "2025-12-14 11:00:00"

    def test_list_of_tuples(self):
        """Test list containing tuples."""
        data = [(1, 2), (3, 4), (5, 6)]
        result = to_json(data)
        parsed = json.loads(result)
        assert parsed == [[1, 2], [3, 4], [5, 6]]


class Color(Enum):
    RED = "red"
    QUOTE = "it's"


class Level(IntEnum):
    HIGH = 2


class Style(StrEnum):
    BOLD = "bold"


class Perm(IntFlag):
    READ = 4


class When(Enum):
    LAUNCH = datetime(2025, 12, 14, 10, 0, 0)


class TestEnumMembers:
    """An Enum member stands for its value: `str()` on one renders `Color.RED`."""

    def test_a_parameter_takes_the_value(self):
        assert to_clickhouse(Color.RED) == "red"
        assert to_clickhouse(Level.HIGH) == 2
        assert to_clickhouse(Style.BOLD) == "bold"
        assert to_clickhouse(Perm.READ) == 4

    def test_a_value_needing_conversion_still_gets_it(self):
        assert to_clickhouse(When.LAUNCH) == "2025-12-14 10:00:00"

    def test_a_member_in_a_container_is_a_literal_of_its_value(self):
        assert to_clickhouse([Color.RED, Level.HIGH]) == "['red',2]"
        assert to_clickhouse((Color.RED,)) == "('red')"
        assert to_clickhouse({"k": Color.RED}) == "{'k':'red'}"

    def test_a_member_used_as_a_map_key_takes_its_value(self):
        assert to_clickhouse({Color.RED: 1}) == "{'red':1}"

    def test_a_value_holding_a_quote_is_still_escaped(self):
        assert to_clickhouse([Color.QUOTE]) == "['it\\'s']"

    def test_json_encodes_the_value(self):
        parsed = json.loads(to_json({"a": Color.RED, "b": Level.HIGH, "c": Style.BOLD, "d": When.LAUNCH}))

        assert parsed == {"a": "red", "b": 2, "c": "bold", "d": "2025-12-14 10:00:00"}


class TestJsonMapKeys:
    """`dumps` offers `default` the values only, so a key it will not take has to be rendered first."""

    def test_a_member_keying_a_map_is_rendered(self):
        assert json.loads(to_json({"m": {Color.RED: 1}})) == {"m": {"red": 1}}

    def test_a_key_whose_value_needs_converting_still_gets_it(self):
        assert json.loads(to_json({"m": {When.LAUNCH: 1}})) == {"m": {"2025-12-14 10:00:00": 1}}

    @pytest.mark.parametrize(
        ("key", "rendered"),
        [
            (UUID("550e8400-e29b-41d4-a716-446655440000"), "550e8400-e29b-41d4-a716-446655440000"),
            (date(2025, 12, 14), "2025-12-14"),
            (Decimal("99.99"), "99.99"),
        ],
    )
    def test_the_other_keys_dumps_rejects(self, key: object, rendered: str):
        """Keying a Map by one of these never worked, Enum or not."""
        assert json.loads(to_json({"m": {key: 1}})) == {"m": {rendered: 1}}

    def test_a_key_nested_below_the_row_is_reached(self):
        assert json.loads(to_json({"rows": [{"m": {Color.RED: 1}}]})) == {"rows": [{"m": {"red": 1}}]}

    def test_rendering_keys_leaves_the_values_alone(self):
        assert json.loads(to_json({"m": {Color.RED: When.LAUNCH}})) == {"m": {"red": "2025-12-14 10:00:00"}}

    def test_a_row_without_such_a_key_is_untouched(self):
        """The walk is the fallback: a row `dumps` takes as it is must never reach it."""
        row = {"id": 1, "ts": datetime(2025, 12, 14, 10, 0, 0), "tags": ["a", "b"]}

        assert json.loads(to_json(row)) == {"id": 1, "ts": "2025-12-14 10:00:00", "tags": ["a", "b"]}


class TestQuoteIdentifier:
    """Tests for identifier quoting, used where the server cannot quote for us."""

    def test_plain_name(self):
        assert quote_identifier("name") == "`name`"

    def test_backquote_is_escaped(self):
        assert quote_identifier("a`b") == "`a\\`b`"

    def test_backslash_is_escaped(self):
        """ClickHouse reads backslash escapes inside backquotes, so the backslash needs one too."""
        assert quote_identifier("a\\b") == "`a\\\\b`"

    def test_backslash_before_backquote(self):
        assert quote_identifier("a\\`b") == "`a\\\\\\`b`"

    def test_injection_attempt_stays_one_name(self):
        assert quote_identifier("a) VALUES (1); DROP TABLE t --") == "`a) VALUES (1); DROP TABLE t --`"
