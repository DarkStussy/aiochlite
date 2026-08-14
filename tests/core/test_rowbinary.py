import asyncio
import ipaddress
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite.converters import rowbinary
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
    server_tz = ZoneInfo("Europe/Moscow")
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


def test_parse_rowbinary_time_and_time64():
    parts = [
        _encode_varuint(4),
        _encode_string("t"),
        _encode_string("t_neg"),
        _encode_string("t64"),
        _encode_string("t64_neg"),
        _encode_string("Time"),
        _encode_string("Time"),
        _encode_string("Time64(3)"),
        _encode_string("Time64(6)"),
        (3661).to_bytes(4, "little", signed=True),
        (-3661).to_bytes(4, "little", signed=True),
        (3_661_123).to_bytes(8, "little", signed=True),
        (-3_661_000_500).to_bytes(8, "little", signed=True),
    ]

    names, types, rows = parse_rowbinary_with_names_and_types(b"".join(parts))
    parsed = list(rows)

    assert names == ["t", "t_neg", "t64", "t64_neg"]
    assert types == ["Time", "Time", "Time64(3)", "Time64(6)"]
    assert parsed[0][0] == timedelta(seconds=3661)
    assert parsed[0][1] == timedelta(seconds=-3661)
    assert parsed[0][2] == timedelta(seconds=3661, microseconds=123_000)
    assert parsed[0][3] == timedelta(seconds=-3661, microseconds=-500)


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


def test_parse_rowbinary_fused_fixed_width_run():
    names, types, rows = parse_rowbinary_with_names_and_types(_fixed_width_payload())

    assert names == ["id", "delta", "flag", "ts", "day", "grade", "name"]
    assert types[0] == "UInt64"
    assert list(rows) == _expected_fixed_width_rows()


def test_parse_rowbinary_fused_run_survives_chunk_splits():
    """A run decoded by a single struct call must still resume across chunk boundaries."""
    payload = _fixed_width_payload()

    async def _chunks():
        for i in range(0, len(payload), 3):
            yield payload[i : i + 3]

    async def _run():
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks())
        await parser.read_header()
        return [row async for row in parser.rows()]

    assert asyncio.run(_run()) == _expected_fixed_width_rows()


def test_parse_rowbinary_fused_and_unfused_agree(monkeypatch: pytest.MonkeyPatch):
    payload = _fixed_width_payload()
    _names, _types, rows = parse_rowbinary_with_names_and_types(payload)
    fused = list(rows)

    monkeypatch.setattr(rowbinary, "_MIN_FUSED_FIELDS", 10_000)
    monkeypatch.setattr(rowbinary, "_MIN_SEGMENTED_FUSED_FIELDS", 10_000)
    _names, _types, rows = parse_rowbinary_with_names_and_types(payload)

    assert fused == list(rows)


def test_streaming_chunk_ends_exactly_after_fused_run():
    """The retry must re-read the whole row, not resume after the already decoded run."""
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


def test_adaptive_cache_stops_and_resumes_caching():
    computed = 0

    def _compute(key: int) -> str:
        nonlocal computed
        computed += 1
        return f"v{key}"

    cached = rowbinary._adaptive_cache(_compute)

    # Unique keys never hit, so the probe window switches the cache off.
    for key in range(rowbinary._CACHE_PROBE_CALLS):
        assert cached(key) == f"v{key}"

    before = computed
    assert cached(0) == "v0"
    assert computed == before + 1

    # After the cooldown it measures again, so repeated keys are served from the cache.
    for _ in range(rowbinary._CACHE_REPROBE_CALLS):
        cached(0)

    assert cached(1) == "v1"
    before = computed
    assert cached(1) == "v1"
    assert computed == before


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
