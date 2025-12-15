import asyncio
import ipaddress
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from aiochlite.converters.rowbinary import (
    RowBinaryWithNamesAndTypesStreamParser,
    parse_rowbinary_with_names_and_types,
    parse_rowbinary_with_names_and_types_lazy,
)


def _encode_varuint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def _encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return _encode_varuint(len(encoded)) + encoded


def test_parse_rowbinary_simple_types() -> None:
    parts = [
        _encode_varuint(2),  # column count
        _encode_string("id"),
        _encode_string("name"),
        _encode_string("UInt8"),
        _encode_string("String"),
        (1).to_bytes(1, "little"),
        _encode_string("alice"),
        (2).to_bytes(1, "little"),
        _encode_string("bob"),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))

    assert names == ["id", "name"]
    assert types == ["UInt8", "String"]
    assert list(rows) == [[1, "alice"], [2, "bob"]]


def test_parse_rowbinary_lazy_only_decodes_accessed_fields() -> None:
    parts = [
        _encode_varuint(2),  # column count
        _encode_string("id"),
        _encode_string("name"),
        _encode_string("UInt8"),
        _encode_string("String"),
        (1).to_bytes(1, "little"),
        _encode_string("alice"),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types_lazy(b"".join(parts))
    row = next(iter(rows))

    assert names == ["id", "name"]
    assert types == ["UInt8", "String"]
    assert row[0] == 1
    assert row[1] == "alice"


def test_streaming_rowbinary_parser_splits_chunks() -> None:
    parts = [
        _encode_varuint(2),
        _encode_string("id"),
        _encode_string("name"),
        _encode_string("UInt8"),
        _encode_string("String"),
        (1).to_bytes(1, "little"),
        _encode_string("alice"),
        (2).to_bytes(1, "little"),
        _encode_string("bob"),
    ]
    payload = b"".join(parts)

    async def _chunks():
        for i in range(0, len(payload), 3):
            yield payload[i : i + 3]

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        names, types = await parser.read_header()
        rows = [row async for row in parser.rows()]
        return names, types, rows

    names, types, rows = asyncio.run(_run())
    assert names == ["id", "name"]
    assert types == ["UInt8", "String"]
    assert rows == [[1, "alice"], [2, "bob"]]


def test_parse_rowbinary_date_and_decimal() -> None:
    base_day = (date(2025, 12, 14) - date(1970, 1, 1)).days
    parts = [
        _encode_varuint(2),
        _encode_string("d"),
        _encode_string("price"),
        _encode_string("Date"),
        _encode_string("Decimal(10, 2)"),
        base_day.to_bytes(2, "little"),
        (12345).to_bytes(8, "little", signed=True),
    ]

    names, _types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["d", "price"]
    assert parsed == [[date(2025, 12, 14), Decimal("123.45")]]


def test_parse_rowbinary_datetime64_array_uuid() -> None:
    epoch_ms = int(datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp() * 1000)
    parts = [
        _encode_varuint(3),
        _encode_string("ts"),
        _encode_string("vals"),
        _encode_string("uid"),
        _encode_string("DateTime64(3, 'UTC')"),
        _encode_string("Array(UInt16)"),
        _encode_string("UUID"),
        epoch_ms.to_bytes(8, "little", signed=True),
        _encode_varuint(3),  # array size
        (1).to_bytes(2, "little"),
        (2).to_bytes(2, "little"),
        (3).to_bytes(2, "little"),
        UUID(int=1).bytes[:8][::-1] + UUID(int=1).bytes[8:][::-1],
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["ts", "vals", "uid"]
    assert types == ["DateTime64(3, 'UTC')", "Array(UInt16)", "UUID"]
    assert parsed[0][0] == datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert parsed[0][1] == [1, 2, 3]
    assert parsed[0][2] == UUID(int=1)


def test_parse_rowbinary_map() -> None:
    parts = [
        _encode_varuint(1),
        _encode_string("m"),
        _encode_string("Map(String, Int32)"),
        _encode_varuint(2),
        _encode_string("a"),
        (1).to_bytes(4, "little", signed=True),
        _encode_string("b"),
        (-2).to_bytes(4, "little", signed=True),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["m"]
    assert types == ["Map(String, Int32)"]
    assert parsed == [[{"a": 1, "b": -2}]]


def test_parse_rowbinary_lowcardinality_wrapper() -> None:
    parts = [
        _encode_varuint(2),
        _encode_string("s"),
        _encode_string("n"),
        _encode_string("LowCardinality(String)"),
        _encode_string("LowCardinality(Nullable(Int32))"),
        _encode_string("x"),
        (0).to_bytes(1, "little"),
        (123).to_bytes(4, "little", signed=True),
        _encode_string("y"),
        (1).to_bytes(1, "little"),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["s", "n"]
    assert types == ["LowCardinality(String)", "LowCardinality(Nullable(Int32))"]
    assert parsed == [["x", 123], ["y", None]]


def test_parse_rowbinary_fixedstring_and_enums() -> None:
    parts = [
        _encode_varuint(3),
        _encode_string("fs"),
        _encode_string("e8"),
        _encode_string("e16"),
        _encode_string("FixedString(4)"),
        _encode_string("Enum8('a' = 1, 'b' = 2)"),
        _encode_string("Enum16('x' = -1, 'y' = 10)"),
        b"ab\x00\x00",
        (2).to_bytes(1, "little", signed=True),
        (-1).to_bytes(2, "little", signed=True),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["fs", "e8", "e16"]
    assert types == ["FixedString(4)", "Enum8('a' = 1, 'b' = 2)", "Enum16('x' = -1, 'y' = 10)"]
    assert parsed == [["ab", "b", "x"]]


def test_parse_rowbinary_ip_types() -> None:
    ipv4 = ipaddress.IPv4Address("1.2.3.4")
    ipv6 = ipaddress.IPv6Address("2001:db8::1")

    parts = [
        _encode_varuint(2),
        _encode_string("ip4"),
        _encode_string("ip6"),
        _encode_string("IPv4"),
        _encode_string("IPv6"),
        int(ipv4).to_bytes(4, "little", signed=False),
        ipv6.packed,
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["ip4", "ip6"]
    assert types == ["IPv4", "IPv6"]
    assert parsed == [[ipv4, ipv6]]


def test_parse_rowbinary_json_type_as_string() -> None:
    parts = [
        _encode_varuint(1),
        _encode_string("doc"),
        _encode_string("JSON"),
        _encode_string('{"a":1,"b":[true,null]}'),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["doc"]
    assert types == ["JSON"]
    assert parsed == [[{"a": 1, "b": [True, None]}]]
