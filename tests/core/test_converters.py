import json
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from aiochlite import from_clickhouse, to_clickhouse, to_json


class TestToClickHouse:
    """Tests for Python to ClickHouse type conversion."""

    def test_basic_types(self):
        """Test basic type conversions."""
        assert to_clickhouse(value=None) == "NULL"
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

    def test_special_types(self):
        """Test UUID and Decimal conversions."""
        uid = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert to_clickhouse(uid) == "550e8400-e29b-41d4-a716-446655440000"

        dec = Decimal("123.456")
        assert to_clickhouse(dec) == "123.456"

    def test_bytes(self):
        """Test bytes conversion."""
        assert to_clickhouse(b"hello") == "hello"


class TestFromClickHouse:
    """Tests for ClickHouse to Python type conversion."""

    def test_datetime_conversion(self):
        """Test DateTime conversion."""
        result = from_clickhouse("2025-12-14 15:30:45", "DateTime")
        assert isinstance(result, datetime)
        assert result == datetime(2025, 12, 14, 15, 30, 45)

    def test_datetime_with_timezone_conversion(self):
        """Test DateTime conversion with timezone modifier."""
        result = from_clickhouse("2025-12-14 15:30:45", "DateTime('UTC')")
        assert isinstance(result, datetime)
        assert result == datetime(2025, 12, 14, 15, 30, 45, tzinfo=ZoneInfo("UTC"))

    def test_datetime64_with_timezone_conversion(self):
        """Test DateTime64 conversion with timezone modifier."""
        result = from_clickhouse("2025-12-14 15:30:45.123456", "Nullable(DateTime64(6, 'Europe/Sofia'))")
        assert isinstance(result, datetime)
        assert result == datetime(2025, 12, 14, 15, 30, 45, 123456, tzinfo=ZoneInfo("Europe/Sofia"))

    def test_date_conversion(self):
        """Test Date conversion."""
        result = from_clickhouse("2025-12-14", "Date")
        assert isinstance(result, date)
        assert result == date(2025, 12, 14)

    def test_uuid_conversion(self):
        """Test UUID conversion."""
        result = from_clickhouse("550e8400-e29b-41d4-a716-446655440000", "UUID")
        assert isinstance(result, UUID)
        assert result == UUID("550e8400-e29b-41d4-a716-446655440000")

    def test_decimal_conversion(self):
        """Test Decimal conversion."""
        result = from_clickhouse("123.456", "Decimal(10, 2)")
        assert isinstance(result, Decimal)
        assert result == Decimal("123.456")

    def test_decimal_variants(self):
        """Test Decimal32/64/128 conversion."""
        result = from_clickhouse("123.45", "Decimal32(2)")
        assert isinstance(result, Decimal)
        assert result == Decimal("123.45")

    def test_nullable_types(self):
        """Test Nullable wrapper."""
        result = from_clickhouse("2025-12-14 15:30:45", "Nullable(DateTime)")
        assert isinstance(result, datetime)

        result = from_clickhouse(None, "Nullable(DateTime)")
        assert result is None

    def test_lowcardinality_types(self):
        """Test LowCardinality wrapper."""
        result = from_clickhouse("2025-12-14", "LowCardinality(Date)")
        assert isinstance(result, date)

    def test_basic_types(self):
        """Test basic types that don't need conversion."""
        assert from_clickhouse(42, "UInt32") == 42
        assert from_clickhouse("test", "String") == "test"
        assert from_clickhouse(3.14, "Float64") == 3.14

    def test_tuple_conversion(self):
        """Test Tuple conversion."""
        result = from_clickhouse(["a", "b", "c"], "Tuple(String, String, String)")
        assert isinstance(result, tuple)
        assert result == ("a", "b", "c")

    def test_tuple_with_types(self):
        """Test Tuple with different types."""
        result = from_clickhouse([1, "test", "2025-12-14"], "Tuple(UInt32, String, Date)")
        assert isinstance(result, tuple)
        assert result[0] == 1
        assert result[1] == "test"
        assert result[2] == date(2025, 12, 14)

    def test_tuple_nested_types(self):
        """Test Tuple with nested Tuple and Array types."""
        ch_type = "Nullable(Tuple(String, Tuple(DateTime('UTC'), UInt8), Array(Nullable(Decimal(10, 2)))))"
        result = from_clickhouse(
            ["abc", ["2025-12-14 10:00:00", 5], ["1.23", None]],
            ch_type,
        )

        assert isinstance(result, tuple)
        assert result[0] == "abc"
        assert result[1] == (datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC")), 5)
        assert result[2] == [Decimal("1.23"), None]

    def test_array_conversion(self):
        """Test Array conversion."""
        result = from_clickhouse([1, 2, 3], "Array(UInt32)")
        assert isinstance(result, list)
        assert result == [1, 2, 3]

    def test_array_with_dates(self):
        """Test Array with Date elements."""
        result = from_clickhouse(["2025-12-14", "2025-12-15"], "Array(Date)")
        assert isinstance(result, list)
        assert all(isinstance(d, date) for d in result)
        assert result == [date(2025, 12, 14), date(2025, 12, 15)]


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
