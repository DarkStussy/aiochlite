from __future__ import annotations

import ipaddress
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_rowbinary_supported_types(ch_client: AsyncChClient):
    query = r"""
        SELECT
            CAST(1 AS Bool) AS b,
            toUInt8(1) AS u8,
            toUInt16(513) AS u16,
            toUInt32(100000) AS u32,
            toUInt64(10000000000) AS u64,
            toInt8(-5) AS i8,
            toInt16(-513) AS i16,
            toInt32(-42) AS i32,
            toInt64(-10000000000) AS i64,
            toFloat32(1.25) AS f32,
            toFloat64(3.5) AS f64,
            CAST('hi' AS String) AS s,
            CAST('ab' AS FixedString(4)) AS fs,
            CAST(2 AS Enum8('a' = 1, 'b' = 2)) AS e8,
            CAST(-1 AS Enum16('x' = -1, 'y' = 10)) AS e16,
            toDate('2025-12-14') AS d,
            toDate32('1900-01-02') AS d32,
            toDateTime('2025-12-14 10:00:00', 'UTC') AS dt,
            toDateTime64('2025-12-14 13:30:45.123456', 6, 'Europe/Moscow') AS dt64,
            CAST('01:01:01' AS Time) AS t,
            CAST('-01:01:01' AS Time) AS t_neg,
            CAST('01:01:01.123456' AS Time64(6)) AS t64,
            CAST('01:01:01.123456789' AS Time64(9)) AS t64_9,
            CAST('123.45' AS Decimal(10, 2)) AS dec,
            CAST('123.45' AS Decimal32(2)) AS dec32,
            CAST('123.45' AS Decimal64(2)) AS dec64,
            CAST('123.45' AS Decimal128(2)) AS dec128,
            CAST('123.45' AS Decimal256(2)) AS dec256,
            toUUID('550e8400-e29b-41d4-a716-446655440000') AS uid,
            toIPv4('1.2.3.4') AS ip4,
            toIPv6('2001:db8::1') AS ip6,
            [toUInt8(1), toUInt8(2), toUInt8(3)] AS arr_u8,
            ['foo', 'bar'] AS arr_s,
            CAST([NULL, 'x', NULL] AS Array(Nullable(String))) AS arr_ns,
            tuple('meta', toInt8(7)) AS t1,
            tuple('meta', toInt8(7), toUInt16(513)) AS t3,
            tuple('meta', toInt8(7), toUInt16(513), toFloat64(0.5)) AS t4,
            map('a', toInt32(1), 'b', toInt32(-2)) AS m,
            toLowCardinality('x') AS lc_s,
            CAST(NULL AS Nullable(String)) AS n_s,
            CAST(NULL AS Nullable(Int32)) AS n_i32,
            CAST('{"a":1,"b":[true,null]}' AS JSON) AS doc_json
        """

    # Time / Time64 are behind the flag until 26.x, where it is on by default.
    row = await ch_client.fetchone(query, settings={"enable_time_time64_type": 1})
    assert row is not None

    assert row["b"] is True

    assert row["u8"] == 1
    assert row["u16"] == 513
    assert row["u32"] == 100000
    assert row["u64"] == 10000000000

    assert row["i8"] == -5
    assert row["i16"] == -513
    assert row["i32"] == -42
    assert row["i64"] == -10000000000

    assert row["f32"] == pytest.approx(1.25)
    assert row["f64"] == 3.5
    assert row["s"] == "hi"
    assert row["fs"] == "ab"
    assert row["e8"] == "b"
    assert row["e16"] == "x"
    assert row["d"] == date(2025, 12, 14)
    assert row["d32"] == date(1900, 1, 2)
    assert row["dt"] == datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert row["dt64"] == datetime(2025, 12, 14, 13, 30, 45, 123456, tzinfo=ZoneInfo("Europe/Moscow"))
    assert row["t"] == timedelta(seconds=3661)
    assert row["t_neg"] == timedelta(seconds=-3661)
    assert row["t64"] == timedelta(seconds=3661, microseconds=123456)
    # Time64(P) carries more precision than timedelta below microseconds, so P > 6 is truncated.
    assert row["t64_9"] == timedelta(seconds=3661, microseconds=123456)
    assert row["dec"] == Decimal("123.45")
    assert row["dec32"] == Decimal("123.45")
    assert row["dec64"] == Decimal("123.45")
    assert row["dec128"] == Decimal("123.45")
    assert row["dec256"] == Decimal("123.45")
    assert row["uid"] == UUID("550e8400-e29b-41d4-a716-446655440000")
    assert row["ip4"] == ipaddress.IPv4Address("1.2.3.4")
    assert row["ip6"] == ipaddress.IPv6Address("2001:db8::1")
    assert row["arr_u8"] == [1, 2, 3]
    assert row["arr_s"] == ["foo", "bar"]
    assert row["arr_ns"] == [None, "x", None]
    assert row["t1"] == ("meta", 7)
    assert row["t3"] == ("meta", 7, 513)
    assert row["t4"] == ("meta", 7, 513, 0.5)
    assert row["m"] == {"a": 1, "b": -2}
    assert row["lc_s"] == "x"
    assert row["n_s"] is None
    assert row["n_i32"] is None
    assert row["doc_json"] == json.loads('{"a":1,"b":[true,null]}')


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_rowbinary_datetime_timezone(ch_client: AsyncChClient):
    # Force the server (session) timezone so the naive wall-clock is deterministic regardless
    # of how the ClickHouse instance is configured.
    query = r"""
        SELECT
            toDateTime('2025-12-14 10:00:00', 'UTC') AS dt_tz,
            CAST(toDateTime('2025-12-14 10:00:00', 'UTC') AS DateTime) AS dt_naive,
            toDateTime64('2025-12-14 10:00:00.500', 3, 'UTC') AS dt64_tz,
            CAST(toDateTime64('2025-12-14 10:00:00.500', 3, 'UTC') AS DateTime64(3)) AS dt64_naive
    """

    row = await ch_client.fetchone(query, settings={"session_timezone": "Europe/Moscow"})
    assert row is not None

    # Explicit timezone in the column type -> timezone-aware datetime.
    assert row["dt_tz"] == datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert row["dt_tz"].tzinfo is not None
    assert row["dt64_tz"] == datetime(2025, 12, 14, 10, 0, 0, 500000, tzinfo=ZoneInfo("UTC"))
    assert row["dt64_tz"].tzinfo is not None

    # No explicit timezone -> naive datetime with the server-timezone wall clock (10:00 UTC = 13:00 MSK).
    assert row["dt_naive"].tzinfo is None
    assert row["dt_naive"] == datetime(2025, 12, 14, 13, 0, 0)
    assert row["dt64_naive"].tzinfo is None
    assert row["dt64_naive"] == datetime(2025, 12, 14, 13, 0, 0, 500000)


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_rowbinary_wide_integers(ch_client: AsyncChClient):
    query = r"""
        SELECT
            toInt128(-5) AS i128,
            toInt128('170141183460469231731687303715884105727') AS i128_max,
            toUInt128('340282366920938463463374607431768211455') AS u128_max,
            toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') AS i256_min,
            toUInt256(9) AS u256,
            [toInt128(-1), toInt128(2)] AS arr_i128,
            CAST(NULL AS Nullable(Int256)) AS n_i256
    """

    row = await ch_client.fetchone(query)
    assert row is not None

    assert row["i128"] == -5
    assert row["i128_max"] == 2**127 - 1
    assert row["u128_max"] == 2**128 - 1
    assert row["i256_min"] == -(2**255)
    assert row["u256"] == 9
    assert row["arr_i128"] == [-1, 2]
    assert row["n_i256"] is None


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_rowbinary_null_literal(ch_client: AsyncChClient):
    """A bare NULL types as Nullable(Nothing), so this is the plainest query there is to break."""
    rows = await ch_client.fetch_rows("SELECT NULL AS v FROM numbers(2)")

    assert rows == [(None,), (None,)]


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_rowbinary_named_tuple(ch_client: AsyncChClient):
    """Names the server really emits, including the ones it backquotes."""
    query = r"""
        SELECT
            tuple(1, 'a')::Tuple(a UInt8, b String) AS named,
            tuple(1)::Tuple(`has space, comma` UInt8) AS backquoted,
            tuple(1)::Tuple(a Enum8('x, y' = 1)) AS enum_label,
            tuple(tuple(1))::Tuple(a Tuple(b UInt8)) AS nested,
            tuple(NULL)::Tuple(a Nullable(UInt8)) AS nullable,
            [tuple(1, 'a')::Tuple(x UInt8, y String)] AS in_array,
            map('k', tuple(1, 'a')::Tuple(x UInt8, y String)) AS in_map
    """

    row = await ch_client.fetchone(query)
    assert row is not None

    assert row["named"] == (1, "a")
    assert row["backquoted"] == (1,)
    assert row["enum_label"] == ("x, y",)
    assert row["nested"] == ((1,),)
    assert row["nullable"] == (None,)
    assert row["in_array"] == [(1, "a")]
    assert row["in_map"] == {"k": (1, "a")}


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_rowbinary_simple_aggregate_function(ch_client: AsyncChClient):
    """The aggregate name is metadata: on the wire the column is its element type."""
    query = r"""
        SELECT
            sumSimpleState(number) AS s,
            anyLastSimpleState(toNullable(number)) AS n,
            groupArrayArraySimpleState([number]) AS arr,
            maxMapSimpleState(map(toUInt8(1), toUInt8(2))) AS m,
            minSimpleState(toString(number)) AS text
        FROM numbers(3)
    """

    row = await ch_client.fetchone(query)
    assert row is not None

    assert row["s"] == 3
    assert row["n"] == 2
    assert row["arr"] == [0, 1, 2]
    assert row["m"] == {1: 2}
    assert row["text"] == "0"
