import asyncio
import ipaddress
import json
import struct
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, AsyncIterator, Callable
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite.converters import rowbinary
from aiochlite.converters.rowbinary import (
    RowBinaryWithNamesAndTypesStreamParser,
    parse_rowbinary_with_names_and_types,
    parse_rowbinary_with_names_and_types_lazy,
)
from aiochlite.exceptions import ChProtocolError


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


def test_parse_rowbinary_simple_types():
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


def test_parse_rowbinary_lazy_only_decodes_accessed_fields():
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


def test_streaming_rowbinary_parser_splits_chunks():
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


def test_parse_rowbinary_date_and_decimal():
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


def test_parse_rowbinary_datetime64_array_uuid():
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


def test_parse_rowbinary_datetime_server_timezone_fallback():
    # The parser receives the timezone name exactly as the server header carries it.
    server_tz = "Europe/Moscow"
    ts = int(datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp())
    epoch_ms = ts * 1000
    parts = [
        _encode_varuint(4),
        _encode_string("dt"),
        _encode_string("dt64"),
        _encode_string("dt_tz"),
        _encode_string("arr"),
        # No explicit timezone -> should fall back to the server timezone.
        _encode_string("DateTime"),
        _encode_string("DateTime64(3)"),
        # Explicit timezone must win over the server timezone.
        _encode_string("DateTime('UTC')"),
        # Nested DateTime should also receive the fallback.
        _encode_string("Array(DateTime)"),
        ts.to_bytes(4, "little"),
        epoch_ms.to_bytes(8, "little", signed=True),
        ts.to_bytes(4, "little"),
        _encode_varuint(1),
        ts.to_bytes(4, "little"),
    ]

    _, _, rows = parse_rowbinary_with_names_and_types(b"".join(parts), server_tz)
    parsed = list(rows)

    # No explicit timezone -> naive datetime with the server-timezone wall clock (as in aiochclient).
    expected_naive = datetime(2025, 12, 14, 13, 0, 0)
    assert parsed[0][0] == expected_naive
    assert parsed[0][0].tzinfo is None
    assert parsed[0][1] == expected_naive
    assert parsed[0][1].tzinfo is None
    # Explicit timezone -> aware datetime.
    assert parsed[0][2] == datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert parsed[0][2].tzinfo == ZoneInfo("UTC")
    assert parsed[0][3] == [expected_naive]
    assert parsed[0][3][0].tzinfo is None


_TIMESTAMP = 1_734_170_400
_MOMENT = datetime(2024, 12, 14, 10, 0, tzinfo=ZoneInfo("UTC"))


def _datetime_payload(ch_type: str) -> bytes:
    value = (
        _TIMESTAMP.to_bytes(4, "little")
        if ch_type.startswith("DateTime(") or ch_type == "DateTime"
        else (_TIMESTAMP * 1000).to_bytes(8, "little", signed=True)
    )
    return b"".join([_encode_varuint(1), _encode_string("dt"), _encode_string(ch_type), value])


def _eager(payload: bytes, server_tz: str | None) -> list[Any]:
    _, _, rows = parse_rowbinary_with_names_and_types(payload, server_tz)
    return [row[0] for row in rows]


def _lazy(payload: bytes, server_tz: str | None) -> list[Any]:
    _, _, rows = parse_rowbinary_with_names_and_types_lazy(payload, server_tz)
    return [row[0] for row in rows]


def _streamed(payload: bytes, server_tz: str | None) -> list[Any]:
    async def _chunks():
        yield payload

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks(), server_tz=server_tz)
        await parser.read_header()
        return [row[0] async for row in parser.rows()]

    return asyncio.run(_run())


Consumer = Callable[[bytes, str | None], list[Any]]

_PARSERS = pytest.mark.parametrize("consume", [_eager, _lazy, _streamed], ids=["eager", "lazy", "stream"])


@_PARSERS
@pytest.mark.parametrize("ch_type", ["DateTime", "DateTime64(3)"])
def test_datetime_without_any_timezone_is_a_protocol_error(ch_type: str, consume: Consumer):
    """Nothing states the wall clock here, and guessing the machine's is how the value goes wrong."""
    with pytest.raises(ChProtocolError, match="X-ClickHouse-Timezone"):
        consume(_datetime_payload(ch_type), None)


@_PARSERS
@pytest.mark.parametrize("ch_type", ["DateTime('UTC')", "DateTime64(3, 'UTC')"])
def test_column_timezone_stands_in_for_a_missing_header(ch_type: str, consume: Consumer):
    assert consume(_datetime_payload(ch_type), None) == [_MOMENT]


def test_unloadable_server_timezone_only_matters_for_datetime():
    """A timezone the runtime cannot load must not quietly become the local one."""
    parts = [
        _encode_varuint(1),
        _encode_string("n"),
        _encode_string("UInt8"),
        (7).to_bytes(1, "little"),
    ]

    _, _, rows = parse_rowbinary_with_names_and_types(b"".join(parts), "Not/AZone")
    assert next(iter(rows))[0] == 7

    parts = [
        _encode_varuint(1),
        _encode_string("dt"),
        _encode_string("DateTime"),
        (1_734_170_400).to_bytes(4, "little"),
    ]

    with pytest.raises(ChProtocolError, match="Not/AZone"):
        list(parse_rowbinary_with_names_and_types(b"".join(parts), "Not/AZone")[2])


def test_parse_rowbinary_datetime64_below_a_microsecond():
    """DateTime64(P > 6) carries digits Python cannot hold, cut toward zero as the server narrows."""
    after = int(datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()) * 10**9 + 123_456_789
    before = int(datetime(1960, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()) * 10**9 + 123_456_789
    parts = [
        _encode_varuint(2),
        _encode_string("after"),
        _encode_string("before"),
        _encode_string("DateTime64(9, 'UTC')"),
        _encode_string("DateTime64(9, 'UTC')"),
        after.to_bytes(8, "little", signed=True),
        before.to_bytes(8, "little", signed=True),
    ]

    _, _, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert parsed[0][0] == datetime(2025, 12, 14, 10, 0, 0, 123_456, tzinfo=ZoneInfo("UTC"))
    # Before the epoch the ticks are negative, so cutting toward zero moves the fraction up.
    assert parsed[0][1] == datetime(1960, 1, 1, 10, 0, 0, 123_457, tzinfo=ZoneInfo("UTC"))


def test_parse_rowbinary_time_and_time64():
    parts = [
        _encode_varuint(6),
        _encode_string("t"),
        _encode_string("t_neg"),
        _encode_string("t64"),
        _encode_string("t64_neg"),
        _encode_string("t64_9"),
        _encode_string("t64_9_neg"),
        _encode_string("Time"),
        _encode_string("Time"),
        _encode_string("Time64(3)"),
        _encode_string("Time64(6)"),
        _encode_string("Time64(9)"),
        _encode_string("Time64(9)"),
        (3661).to_bytes(4, "little", signed=True),
        (-3661).to_bytes(4, "little", signed=True),
        (3_661_123).to_bytes(8, "little", signed=True),
        (-3_661_000_500).to_bytes(8, "little", signed=True),
        (3_661_123_456_789).to_bytes(8, "little", signed=True),
        (-3_661_123_456_789).to_bytes(8, "little", signed=True),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["t", "t_neg", "t64", "t64_neg", "t64_9", "t64_9_neg"]
    assert types == ["Time", "Time", "Time64(3)", "Time64(6)", "Time64(9)", "Time64(9)"]
    assert parsed[0][0] == timedelta(seconds=3661)
    assert parsed[0][1] == timedelta(seconds=-3661)
    assert parsed[0][2] == timedelta(seconds=3661, microseconds=123_000)
    assert parsed[0][3] == timedelta(seconds=-3661, microseconds=-500)
    # Below a microsecond the value is cut toward zero, symmetrically and as the server narrows it.
    assert parsed[0][4] == timedelta(seconds=3661, microseconds=123_456)
    assert parsed[0][5] == timedelta(seconds=-3661, microseconds=-123_456)


def test_parse_rowbinary_time64_at_the_microsecond_boundary():
    """A tick short of a microsecond must vanish on both sides rather than round outward."""
    ticks = [999, 1000, -999, -1000]
    parts = [
        _encode_varuint(len(ticks)),
        *(_encode_string(f"t{index}") for index in range(len(ticks))),
        *(_encode_string("Time64(9)") for _ in ticks),
        *(value.to_bytes(8, "little", signed=True) for value in ticks),
    ]

    _, _, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert list(parsed[0]) == [
        timedelta(0),
        timedelta(microseconds=1),
        timedelta(0),
        timedelta(microseconds=-1),
    ]


def test_parse_rowbinary_time_in_array_and_nullable():
    parts = [
        _encode_varuint(2),
        _encode_string("arr"),
        _encode_string("nt64"),
        _encode_string("Array(Time)"),
        _encode_string("Nullable(Time64(3))"),
        _encode_varuint(2),
        (60).to_bytes(4, "little", signed=True),
        (-60).to_bytes(4, "little", signed=True),
        (0).to_bytes(1, "little"),
        (1_500).to_bytes(8, "little", signed=True),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["arr", "nt64"]
    assert types == ["Array(Time)", "Nullable(Time64(3))"]
    assert parsed[0][0] == [timedelta(seconds=60), timedelta(seconds=-60)]
    assert parsed[0][1] == timedelta(seconds=1, microseconds=500_000)


def test_parse_rowbinary_map():
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


def test_parse_rowbinary_lowcardinality_wrapper():
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


def test_parse_rowbinary_fixedstring_and_enums():
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


def test_parse_rowbinary_ip_types():
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


def test_parse_rowbinary_json_type_as_string():
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


JSON_DOCUMENTS = [
    '{"a":1,"b":[true,null]}',
    "{}",
    "[]",
    "42",
    '"hi"',
    "null",
    # Where the scanner alone would differ from `loads`.
    '{"v": 18446744073709551615}',
    '{"v": "\\ud800"}',
    '{"a":' * 200 + "1" + "}" * 200,
    '  {"led": "by whitespace"}',
]


@pytest.mark.parametrize("document", JSON_DOCUMENTS)
def test_json_column_decodes_as_the_standard_library_would(document: str):
    """The decoder reaches the C scanner directly, skipping what `loads` checks around it."""
    parts = [_encode_varuint(1), _encode_string("doc"), _encode_string("JSON"), _encode_string(document)]

    _names, _types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))

    assert [row[0] for row in rows] == [json.loads(document)]


@pytest.mark.parametrize("document", ["", "{oops}", '{"a": ', "  "])
def test_a_malformed_json_column_raises_what_the_standard_library_raises(document: str):
    """The scanner signals these by StopIteration, which must not reach the caller."""
    parts = [_encode_varuint(1), _encode_string("doc"), _encode_string("JSON"), _encode_string(document)]

    with pytest.raises(json.JSONDecodeError):
        list(parse_rowbinary_with_names_and_types(b"".join(parts))[2])


def _fixed_width_parts() -> tuple[bytes, list[tuple[bytes, bytes]]]:
    """Header plus per-row (fixed-width part, trailing String) pairs."""
    base_day = (date(2025, 12, 14) - date(1970, 1, 1)).days
    header = b"".join(
        [
            _encode_varuint(7),
            _encode_string("id"),
            _encode_string("delta"),
            _encode_string("flag"),
            _encode_string("ts"),
            _encode_string("day"),
            _encode_string("grade"),
            _encode_string("name"),
            _encode_string("UInt64"),
            _encode_string("Int32"),
            _encode_string("Bool"),
            _encode_string("DateTime('UTC')"),
            _encode_string("Date"),
            _encode_string("Enum8('a' = 1, 'b' = 2)"),
            _encode_string("String"),
        ]
    )
    rows = [
        (
            b"".join(
                [
                    (10 + row).to_bytes(8, "little"),
                    (-5 - row).to_bytes(4, "little", signed=True),
                    (row % 2).to_bytes(1, "little"),
                    (1734160800 + row).to_bytes(4, "little"),
                    (base_day + row).to_bytes(2, "little"),
                    (1 + row).to_bytes(1, "little", signed=True),
                ]
            ),
            _encode_string(f"row{row}"),
        )
        for row in range(2)
    ]

    return header, rows


def _fixed_width_payload() -> bytes:
    """Build a two-row payload whose columns are all fixed-width, plus a trailing String."""
    header, rows = _fixed_width_parts()
    return header + b"".join(chunk for row in rows for chunk in row)


def _expected_fixed_width_rows() -> list[list[object]]:
    utc = ZoneInfo("UTC")
    return [
        [
            10 + row,
            -5 - row,
            bool(row % 2),
            datetime.fromtimestamp(1734160800 + row, tz=utc),
            date(2025, 12, 14) + timedelta(days=row),
            "a" if row == 0 else "b",
            f"row{row}",
        ]
        for row in range(2)
    ]


def test_parse_rowbinary_mixed_fixed_and_string_row():
    names, types, rows = parse_rowbinary_with_names_and_types(_fixed_width_payload())

    assert names == ["id", "delta", "flag", "ts", "day", "grade", "name"]
    assert types[0] == "UInt64"
    assert list(rows) == _expected_fixed_width_rows()


def test_parse_rowbinary_mixed_row_survives_chunk_splits():
    """A row decoded in one pass must still resume across chunk boundaries."""
    payload = _fixed_width_payload()

    async def _chunks():
        for i in range(0, len(payload), 3):
            yield payload[i : i + 3]

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        await parser.read_header()
        return [row async for row in parser.rows()]

    assert asyncio.run(_run()) == _expected_fixed_width_rows()


def test_compiled_and_reader_paths_agree_on_a_mixed_row(monkeypatch: pytest.MonkeyPatch):
    payload = _fixed_width_payload()
    _names, _types, rows = parse_rowbinary_with_names_and_types(payload)
    compiled = list(rows)

    monkeypatch.setattr(rowbinary, "_fixed_row_layout", lambda *_, **__: None)
    monkeypatch.setattr(rowbinary, "_compiled_row_decoder", lambda *_, **__: None)
    _names, _types, rows = parse_rowbinary_with_names_and_types(payload)

    assert compiled == list(rows)


def test_streaming_chunk_ends_between_columns_of_a_row():
    """The retry must re-read the whole row, not resume where the chunk ran out."""
    header, rows = _fixed_width_parts()
    first_fixed, first_name = rows[0]
    chunks = [header + first_fixed, first_name + rows[1][0] + rows[1][1]]

    async def _chunks():
        for chunk in chunks:
            yield chunk

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        await parser.read_header()
        return [row async for row in parser.rows()]

    assert asyncio.run(_run()) == _expected_fixed_width_rows()


FIXED_WIDTH_TYPES = [
    "Bool",
    "UInt8",
    "Int16",
    "UInt32",
    "Int64",
    "Float32",
    "Float64",
    "Date",
    "Date32",
    "DateTime('UTC')",
    "DateTime64(3, 'UTC')",
    "Time",
    "Time64(6)",
    "Enum8('a' = 1)",
    "Enum16('a' = 1)",
    "IPv4",
    "IPv6",
    "UUID",
    "FixedString(5)",
    "Decimal32(2)",
    "Decimal64(2)",
    "Decimal128(2)",
    "Decimal256(2)",
    "LowCardinality(UInt64)",
]

VARIABLE_WIDTH_TYPES = [
    "String",
    "JSON",
    "Array(UInt8)",
    "Map(String, UInt8)",
    "Tuple(String, UInt8)",
    "Nullable(UInt64)",
    "LowCardinality(Nullable(UInt64))",
]


@pytest.mark.parametrize("ch_type", FIXED_WIDTH_TYPES)
def test_fixed_width_matches_the_skipper(ch_type: str):
    """A wrong width would silently desynchronize the whole lazy stream."""
    width = rowbinary._fixed_width(ch_type)
    reader = rowbinary._BinaryReader(bytes(64))
    rowbinary._skipper_for_type(ch_type)(reader)

    assert width == reader.pos


@pytest.mark.parametrize("ch_type", VARIABLE_WIDTH_TYPES)
def test_variable_width_types_have_no_fixed_width(ch_type: str):
    assert rowbinary._fixed_width(ch_type) is None


@pytest.mark.parametrize("ch_type", FIXED_WIDTH_TYPES)
def test_every_fixed_width_type_has_a_struct_code_of_that_width(ch_type: str):
    """A code of the wrong width would desynchronize every row after the first."""
    field = rowbinary._fixed_field(ch_type, "UTC")

    assert field is not None
    assert struct.calcsize(f"<{field[0]}") == rowbinary._fixed_width(ch_type)


def _fixed_only_payload() -> bytes:
    parts = [
        _encode_varuint(3),
        _encode_string("id"),
        _encode_string("ts"),
        _encode_string("price"),
        _encode_string("UInt64"),
        _encode_string("DateTime('UTC')"),
        _encode_string("Decimal64(2)"),
    ]
    for row in range(3):
        parts += [
            (10 + row).to_bytes(8, "little"),
            (1734160800 + row).to_bytes(4, "little"),
            (12345 + row).to_bytes(8, "little", signed=True),
        ]

    return b"".join(parts)


def _expected_fixed_only_rows() -> list[list[object]]:
    utc = ZoneInfo("UTC")
    return [
        [10 + row, datetime.fromtimestamp(1734160800 + row, tz=utc), Decimal(12345 + row).scaleb(-2)]
        for row in range(3)
    ]


def test_lazy_rows_of_constant_width_share_offsets():
    payload = _fixed_only_payload()
    _names, types, rows = parse_rowbinary_with_names_and_types_lazy(payload)

    assert rowbinary._lazy_row_template(types) is not None
    assert [[row[0], row[1], row[2]] for row in rows] == _expected_fixed_only_rows()


def test_lazy_constant_width_rows_survive_chunk_splits():
    payload = _fixed_only_payload()

    async def _chunks():
        for i in range(0, len(payload), 5):
            yield payload[i : i + 5]

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks(), lazy=True)
        await parser.read_header()
        return [[row[0], row[1], row[2]] async for row in parser.rows()]

    assert asyncio.run(_run()) == _expected_fixed_only_rows()


def test_bulk_decode_handles_a_fully_fixed_width_payload():
    payload = _fixed_only_payload()
    names, types, rows = parse_rowbinary_with_names_and_types(payload)

    assert rowbinary._fixed_row_layout(types, None) is not None
    assert names == ["id", "ts", "price"]
    assert list(rows) == _expected_fixed_only_rows()


def _parse_rows(payload: bytes, *, as_tuple: bool) -> list[Any]:
    """Decode a payload, picking the overload a runtime flag cannot."""
    if as_tuple:
        return list(parse_rowbinary_with_names_and_types(payload, as_tuple=True)[2])

    return list(parse_rowbinary_with_names_and_types(payload, as_tuple=False)[2])


@pytest.mark.parametrize("as_tuple", [False, True])
def test_bulk_and_reader_paths_agree(as_tuple: bool, monkeypatch: pytest.MonkeyPatch):
    """The bulk pass must decode exactly what the per-row reader does."""
    payload = _fixed_only_payload()
    bulk = _parse_rows(payload, as_tuple=as_tuple)

    # No layout is what a variable-width column reports, and what selects the path.
    monkeypatch.setattr(rowbinary, "_fixed_row_layout", lambda *_: None)

    assert bulk == _parse_rows(payload, as_tuple=as_tuple)
    assert all(isinstance(row, tuple if as_tuple else list) for row in bulk)


def test_bulk_decode_rejects_a_truncated_row():
    """A partial trailing row must surface as a decode failure."""
    payload = _fixed_only_payload()[:-4]

    with pytest.raises(ValueError, match="Unexpected end of data"):
        list(parse_rowbinary_with_names_and_types(payload)[2])


def test_bulk_decode_accepts_a_payload_with_no_rows():
    # 3 rows of UInt64 + DateTime + Decimal64.
    header = _fixed_only_payload()[: -3 * 20]
    _names, _types, rows = parse_rowbinary_with_names_and_types(header)

    assert list(rows) == []


@pytest.mark.parametrize("ch_type", VARIABLE_WIDTH_TYPES)
def test_a_variable_width_column_keeps_the_reader_path(ch_type: str):
    assert rowbinary._fixed_row_layout(["UInt64", ch_type], "UTC") is None


def test_streaming_bulk_rows_survive_every_chunk_boundary():
    """A row split across chunks must wait for the rest of it."""
    payload = _fixed_only_payload()

    async def _run(size: int):
        async def _chunks():
            for i in range(0, len(payload), size):
                yield payload[i : i + size]

        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        await parser.read_header()
        assert parser._batch is not None
        return [row async for row in parser.rows()]

    expected = _expected_fixed_only_rows()
    assert all(asyncio.run(_run(size)) == expected for size in range(1, len(payload) + 2))


def test_streaming_bulk_reports_a_truncated_trailing_row():
    payload = _fixed_only_payload()[:-4]

    async def _chunks():
        yield payload

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        await parser.read_header()
        return [row async for row in parser.rows()]

    with pytest.raises(ValueError, match="Unexpected end of data"):
        asyncio.run(_run())


def test_streaming_batches_a_row_of_fixed_and_string_columns():
    """A String column keeps the row off the bulk path but not off the batched one."""
    payload = _fixed_width_payload()

    async def _chunks():
        yield payload

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        types = (await parser.read_header())[1]
        assert rowbinary._fixed_row_layout(types, None) is None
        assert parser._batch is not None
        return [row async for row in parser.rows()]

    assert asyncio.run(_run()) == _expected_fixed_width_rows()


def test_streaming_keeps_the_reader_path_when_no_column_is_covered(monkeypatch: pytest.MonkeyPatch):
    """Which types compile is tested separately; this is the path a row takes when none does."""
    monkeypatch.setattr(rowbinary, "_compiled_row_decoder", lambda *_, **__: None)
    parts = [
        _encode_varuint(1),
        _encode_string("tags"),
        _encode_string("Map(String, Array(UInt8))"),
        _encode_varuint(1),
        _encode_string("hi"),
        _encode_varuint(2),
        (5).to_bytes(1, "little"),
        (6).to_bytes(1, "little"),
    ]
    payload = b"".join(parts)

    async def _chunks():
        yield payload

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        await parser.read_header()
        assert parser._batch is None
        return [row async for row in parser.rows()]

    assert asyncio.run(_run()) == [[{"hi": [5, 6]}]]


def _encode_nested(arrays: list[list[Any]], pack: Callable[[Any], bytes]) -> bytes:
    return b"".join(
        [_encode_varuint(len(arrays))]
        + [_encode_varuint(len(inner)) + b"".join(pack(value) for value in inner) for inner in arrays]
    )


def test_streaming_reader_reads_every_fixed_width_type_across_chunk_splits(monkeypatch: pytest.MonkeyPatch):
    """Every per-type read of `_StreamingReader`, which serves the columns the generator skips.

    The generator is switched off rather than fed types it cannot emit, so the reads stay covered
    however much of the type space it grows to cover.
    """
    monkeypatch.setattr(rowbinary, "_compiled_row_decoder", lambda *_, **__: None)
    widths = [("UInt8", 1, False), ("Int8", 1, True), ("UInt16", 2, False), ("Int16", 2, True)]
    widths += [("UInt32", 4, False), ("Int32", 4, True), ("UInt64", 8, False), ("Int64", 8, True)]
    types = [f"Array(Array({name}))" for name, _size, _signed in widths]
    types += ["Array(Array(Float32))", "Array(Array(Float64))", "Array(Array(FixedString(3)))"]

    def _pack_int(size: int, signed: bool) -> Callable[[Any], bytes]:
        return lambda value: int(value).to_bytes(size, "little", signed=signed)

    packers: list[Callable[[Any], bytes]] = [_pack_int(size, signed) for _name, size, signed in widths]
    packers += [lambda v: struct.pack("<f", v), lambda v: struct.pack("<d", v), str.encode]

    integers: list[list[list[Any]]] = [[[1, 2], [3]], [[-1], []]] * (len(widths) // 2)
    rows: list[list[Any]] = [
        [*integers, [[0.5]], [[1.25, -2.5]], [["abc"]]],
        [[[7]], [[-7]], [[8]], [[-8]], [[9]], [[-9]], [[10]], [[-10]], [[]], [[3.5]], [["xyz", "123"]]],
    ]

    parts = [_encode_varuint(len(types))]
    parts += [_encode_string(f"c{index}") for index in range(len(types))]
    parts += [_encode_string(ch_type) for ch_type in types]
    parts += [_encode_nested(value, pack) for row in rows for value, pack in zip(row, packers, strict=True)]
    payload = b"".join(parts)

    assert rowbinary._batch_decoder(types, None) is None

    for size in (1, 5, 17, len(payload)):

        async def _run(size: int = size) -> list[Any]:
            async def _chunks() -> AsyncIterator[bytes]:
                for start in range(0, len(payload), size):
                    yield payload[start : start + size]

            parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
            await parser.read_header()
            assert parser._batch is None
            return [row async for row in parser.rows()]

        assert asyncio.run(_run()) == rows, f"chunk size {size}"


def test_value_cache_converts_once_per_distinct_value():
    calls = []

    def _convert(value: int) -> str:
        calls.append(value)
        return str(value)

    cached = rowbinary._value_cache(_convert)

    assert [cached(v) for v in (1, 2, 1, 2, 3)] == ["1", "2", "1", "2", "3"]
    assert calls == [1, 2, 3]


def test_value_cache_keeps_entries_past_the_bound_of_the_shared_cache():
    """A bound is what made the cache collapse on a column of middling cardinality."""
    converted = 0

    def _convert(value: int) -> int:
        nonlocal converted
        converted += 1
        return value

    cached = rowbinary._value_cache(_convert)
    values = range(rowbinary._VALUE_CACHE_SIZE * 2)
    for value in values:
        cached(value)
    for value in values:
        cached(value)

    assert converted == len(values)


def test_value_cache_stays_within_its_bound(monkeypatch: pytest.MonkeyPatch):
    """`stream()` holds the cache for the whole result, so it cannot grow with it."""
    monkeypatch.setattr(rowbinary, "_QUERY_VALUE_CACHE_SIZE", 4)
    cache = rowbinary._ValueCache(str)

    for value in range(100):
        assert cache[value] == str(value)
        assert len(cache) <= 4


def test_value_cache_full_starts_over_rather_than_freezing(monkeypatch: pytest.MonkeyPatch):
    """Freezing keeps whatever it saw first, so a working set that moves would miss forever."""
    monkeypatch.setattr(rowbinary, "_QUERY_VALUE_CACHE_SIZE", 4)
    cache = rowbinary._ValueCache(str)

    for value in range(4):
        cache[value]
    cache[100]

    assert dict(cache) == {100: "100"}


CODEGEN_SCHEMAS = [
    ["String"],
    ["UInt64", "String"],
    ["String", "UInt64"],
    ["String", "String"],
    ["UInt64", "Float64", "String"],
    ["String", "DateTime('UTC')", "String"],
    ["LowCardinality(String)", "Int32"],
    ["Bool", "String", "Decimal64(2)", "String", "Date"],
    ["Nullable(UInt64)"],
    ["Nullable(String)"],
    ["Nullable(UInt64)", "Nullable(String)", "Int32"],
    ["UInt64", "Nullable(DateTime('UTC'))", "String"],
    ["LowCardinality(Nullable(String))", "Nullable(Decimal64(2))"],
    ["UUID", "String"],
    ["FixedString(5)", "UInt64"],
    ["Decimal128(2)", "String", "IPv6"],
    ["Nullable(UUID)", "Nullable(FixedString(5))"],
    ["Array(UInt64)"],
    ["Array(String)"],
    ["UInt64", "Array(UInt64)", "String"],
    ["Array(DateTime('UTC'))", "Array(UUID)"],
    ["Array(String)", "Nullable(String)", "Array(Decimal64(2))"],
    # Mixed rows: the uncovered column reads through its closure, the rest is emitted inline.
    ["UInt64", "Tuple(String, UInt8)", "String"],
    ["UInt64", "Map(String, UInt8)", "DateTime('UTC')"],
    ["String", "JSON", "UInt64"],
    ["UInt64", "Array(Array(UInt8))", "Nullable(String)"],
    ["Tuple(String, UInt8)", "Map(String, UInt8)", "UInt64"],
    ["Tuple(String, UInt8)"],
    ["Tuple(UInt64, UInt64)", "String"],
    ["UInt64", "Tuple(String, DateTime('UTC'))", "Array(UInt64)"],
    ["Map(String, UInt8)"],
    ["UInt64", "Map(String, UInt64)", "String"],
    ["Map(String, DateTime('UTC'))", "Nullable(String)"],
    # Containers holding a container or a Nullable, which used to fall to the reader.
    ["Array(Nullable(UInt64))"],
    ["Array(Nullable(String))"],
    ["UInt64", "Array(Nullable(DateTime('UTC')))", "String"],
    ["Array(Array(UInt8))"],
    ["Array(Array(String))", "UInt64"],
    ["Array(Tuple(String, UInt8))"],
    ["Array(Map(String, UInt8))", "String"],
    ["Tuple(Nullable(UInt8), String)"],
    ["Tuple(String, Array(UInt8))", "UInt64"],
    ["Tuple(Tuple(UInt8, UInt8), String)"],
    ["Map(String, Nullable(UInt8))"],
    ["Map(String, Array(UInt8))", "Nullable(String)"],
    ["Map(String, Map(String, UInt8))"],
]

# Empty, one byte, exactly at and either side of the single-byte varint limit, and multi-byte.
CODEGEN_STRINGS = ["", "x", "ünïcødé ✓", "a" * 126, "b" * 127, "c" * 128, "d" * 129, "e" * 500]


def _array(i: int, encode: Callable[[int], bytes]) -> bytes:
    count = i % 4
    return _encode_varuint(count) + b"".join(encode(i + j) for j in range(count))


def _nullable(encode: Callable[[int], bytes]) -> Callable[[int], bytes]:
    """Every third row null, so both branches of the column are exercised."""
    return lambda i: b"\x01" if i % 3 == 0 else b"\x00" + encode(i)


def _string(i: int) -> bytes:
    return _encode_string(CODEGEN_STRINGS[i % len(CODEGEN_STRINGS)])


def _uint64(i: int) -> bytes:
    return i.to_bytes(8, "little")


def _datetime_utc(i: int) -> bytes:
    return (1734160800 + i).to_bytes(4, "little")


def _decimal64(i: int) -> bytes:
    return (i * 7 - 3).to_bytes(8, "little", signed=True)


def _uuid(i: int) -> bytes:
    return (i + 1).to_bytes(8, "little") + (i + 2).to_bytes(8, "little")


def _fixedstring5(i: int) -> bytes:
    return f"ab{i % 10}".encode().ljust(5, b"\x00")


def _codegen_payload(types: list[str], rows: int) -> bytes:
    values: dict[str, Callable[[int], bytes]] = {
        "String": _string,
        "LowCardinality(String)": lambda i: _encode_string(f"cat-{i % 3}"),
        "UInt64": _uint64,
        "Int32": lambda i: (-i).to_bytes(4, "little", signed=True),
        "Float64": lambda i: struct.pack("<d", i * 1.5),
        "Bool": lambda i: bytes([i % 2]),
        "Date": lambda i: (20000 + i).to_bytes(2, "little"),
        "DateTime('UTC')": _datetime_utc,
        "Decimal64(2)": _decimal64,
        "Nullable(UInt64)": _nullable(_uint64),
        "Nullable(String)": _nullable(_string),
        "Nullable(DateTime('UTC'))": _nullable(_datetime_utc),
        "Nullable(Decimal64(2))": _nullable(_decimal64),
        "LowCardinality(Nullable(String))": _nullable(_string),
        "UUID": _uuid,
        "FixedString(5)": _fixedstring5,
        "IPv6": lambda i: bytes([i % 256]) + bytes(15),
        "Decimal128(2)": lambda i: (i * 11 - 5).to_bytes(16, "little", signed=True),
        "Nullable(UUID)": _nullable(_uuid),
        "Nullable(FixedString(5))": _nullable(_fixedstring5),
        # Lengths 0..3, so an empty array and the count varint are both exercised.
        "Array(UInt64)": lambda i: _array(i, _uint64),
        "Array(String)": lambda i: _array(i, _string),
        "Array(DateTime('UTC'))": lambda i: _array(i, _datetime_utc),
        "Array(UUID)": lambda i: _array(i, _uuid),
        "Array(Decimal64(2))": lambda i: _array(i, _decimal64),
        "Tuple(String, UInt8)": lambda i: _string(i) + bytes([i % 256]),
        "Tuple(UInt64, UInt64)": lambda i: _uint64(i) + _uint64(i + 1),
        "Tuple(String, DateTime('UTC'))": lambda i: _string(i) + _datetime_utc(i),
        "Map(String, UInt8)": lambda i: (
            _encode_varuint(i % 3) + b"".join(_encode_string(f"k{j}") + bytes([j]) for j in range(i % 3))
        ),
        "Map(String, UInt64)": lambda i: (
            _encode_varuint(i % 4) + b"".join(_encode_string(f"k{j}") + _uint64(i + j) for j in range(i % 4))
        ),
        "Map(String, DateTime('UTC'))": lambda i: (
            _encode_varuint(i % 3) + b"".join(_encode_string(f"k{j}") + _datetime_utc(i + j) for j in range(i % 3))
        ),
        "JSON": lambda i: _encode_string(f'{{"a": {i}, "b": "x{i % 7}"}}'),
        "Array(Array(UInt8))": lambda i: (
            _encode_varuint(i % 3) + b"".join(_encode_varuint(j) + bytes(range(j)) for j in range(i % 3))
        ),
        # Containers the generator now walks itself rather than handing to a closure.
        "Array(Nullable(UInt64))": lambda i: _array(i, _nullable(_uint64)),
        "Array(Nullable(String))": lambda i: _array(i, _nullable(_string)),
        "Array(Nullable(DateTime('UTC')))": lambda i: _array(i, _nullable(_datetime_utc)),
        "Array(Array(String))": lambda i: _encode_varuint(i % 3) + b"".join(_array(j, _string) for j in range(i % 3)),
        "Array(Tuple(String, UInt8))": lambda i: _array(i, lambda j: _string(j) + bytes([j % 256])),
        "Array(Map(String, UInt8))": lambda i: _array(
            i, lambda j: _encode_varuint(j % 3) + b"".join(_encode_string(f"k{n}") + bytes([n]) for n in range(j % 3))
        ),
        "Tuple(Nullable(UInt8), String)": lambda i: _nullable(lambda j: bytes([j % 256]))(i) + _string(i),
        "Tuple(String, Array(UInt8))": lambda i: _string(i) + _array(i, lambda j: bytes([j % 256])),
        "Tuple(Tuple(UInt8, UInt8), String)": lambda i: bytes([i % 256, (i + 1) % 256]) + _string(i),
        "Map(String, Nullable(UInt8))": lambda i: (
            _encode_varuint(i % 3)
            + b"".join(_encode_string(f"k{j}") + _nullable(lambda n: bytes([n % 256]))(j) for j in range(i % 3))
        ),
        "Map(String, Array(UInt8))": lambda i: (
            _encode_varuint(i % 3)
            + b"".join(_encode_string(f"k{j}") + _array(j, lambda n: bytes([n % 256])) for j in range(i % 3))
        ),
        "Map(String, Map(String, UInt8))": lambda i: (
            _encode_varuint(i % 3)
            + b"".join(
                _encode_string(f"k{j}")
                + _encode_varuint(j % 2)
                + b"".join(_encode_string(f"n{n}") + bytes([n]) for n in range(j % 2))
                for j in range(i % 3)
            )
        ),
    }
    header = [_encode_varuint(len(types))]
    header += [_encode_string(f"c{idx}") for idx in range(len(types))]
    header += [_encode_string(ch_type) for ch_type in types]

    return b"".join(header) + b"".join(b"".join(values[t](i) for t in types) for i in range(rows))


@pytest.mark.parametrize("types", CODEGEN_SCHEMAS, ids=",".join)
@pytest.mark.parametrize("as_tuple", [False, True])
def test_compiled_and_reader_paths_agree(types: list[str], as_tuple: bool, monkeypatch: pytest.MonkeyPatch):
    """The compiled decoder must produce exactly what the per-field reader does."""
    payload = _codegen_payload(types, len(CODEGEN_STRINGS))
    compiled = _parse_rows(payload, as_tuple=as_tuple)

    monkeypatch.setattr(rowbinary, "_compiled_row_decoder", lambda *_, **__: None)

    assert compiled == _parse_rows(payload, as_tuple=as_tuple)
    assert all(isinstance(row, tuple if as_tuple else list) for row in compiled)


@pytest.mark.parametrize("cut", range(1, 30))
def test_compiled_decoder_rejects_a_truncated_row(cut: int):
    """Stopping on a row boundary must be reported, not returned as a short result."""
    payload = _codegen_payload(["UInt64", "String", "DateTime('UTC')"], 4)

    with pytest.raises(ValueError, match="Unexpected end of data"):
        list(parse_rowbinary_with_names_and_types(payload[:-cut])[2])


UNCOVERED_TYPES = [
    "JSON",
    "Array(JSON)",
    "Map(String, JSON)",
    # Past `_MAX_CODEGEN_DEPTH`, where each further level would multiply the code emitted.
    "Array(Array(Array(Array(Array(UInt8)))))",
    "Map(String, Array(Array(Array(Array(UInt8)))))",
]


@pytest.mark.parametrize("ch_type", UNCOVERED_TYPES)
def test_an_uncovered_column_falls_back_to_its_own_reader(ch_type: str):
    """One column the generator cannot emit must not cost the row the compiled path."""
    compiled = rowbinary._compiled_row_decoder(("UInt64", ch_type), "UTC", as_tuple=True)

    assert compiled is not None
    assert "_f1" in compiled[1]
    assert "_f0" not in compiled[1]


@pytest.mark.parametrize("ch_type", UNCOVERED_TYPES)
def test_a_row_of_only_uncovered_columns_is_not_compiled(ch_type: str):
    """With nothing to emit inline the loop would only wrap the reader calls it already makes."""
    assert rowbinary._compiled_row_decoder((ch_type,), "UTC", as_tuple=True) is None


def test_compiled_cache_holds_no_converters():
    """Converters memoize per query, so caching one would pin every decoded value."""
    compiled = rowbinary._compiled_row_decoder(("DateTime('UTC')", "String"), "UTC", as_tuple=True)

    assert compiled is not None
    _code, schema_globals, converters = compiled
    # The slot's type is kept, so the converter itself can be rebuilt per query.
    assert converters == (("0", "DateTime('UTC')"),)
    assert not [name for name in schema_globals if name.startswith("_c")]


@pytest.mark.parametrize(
    ("method", "size"),
    [
        ("read_uint16", 2),
        ("read_int16", 2),
        ("read_uint32", 4),
        ("read_int32", 4),
        ("read_uint64", 8),
        ("read_int64", 8),
        ("read_float32", 4),
        ("read_float64", 8),
    ],
)
def test_reading_past_the_end_raises_a_decode_error(method: str, size: int):
    """`struct.error` is no ValueError, so an unchecked read escaped the decode boundary."""
    reader = rowbinary._BinaryReader(bytes(size - 1))

    with pytest.raises(ValueError, match="Unexpected end of data"):
        getattr(reader, method)()
