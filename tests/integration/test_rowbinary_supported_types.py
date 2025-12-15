from __future__ import annotations

import ipaddress
import json
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


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
            map('a', toInt32(1), 'b', toInt32(-2)) AS m,
            toLowCardinality('x') AS lc_s,
            CAST(NULL AS Nullable(String)) AS n_s,
            CAST(NULL AS Nullable(Int32)) AS n_i32,
            CAST('{"a":1,"b":[true,null]}' AS JSON) AS doc_json
        """

    row = await ch_client.fetchone(query)
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
    assert row["m"] == {"a": 1, "b": -2}
    assert row["lc_s"] == "x"
    assert row["n_s"] is None
    assert row["n_i32"] is None
    assert row["doc_json"] == json.loads('{"a":1,"b":[true,null]}')
