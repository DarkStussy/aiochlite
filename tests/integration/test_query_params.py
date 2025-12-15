from __future__ import annotations

import ipaddress
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


async def test_settings_are_applied(ch_client: AsyncChClient):
    value = await ch_client.fetchval(
        "SELECT toUInt64(getSetting('max_result_rows'))",
        settings={"max_result_rows": 123},
    )
    assert value == 123


async def test_params_scalar_types(ch_client: AsyncChClient):
    uid = UUID("550e8400-e29b-41d4-a716-446655440000")
    dt = datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    d = date(2025, 12, 14)
    ip4 = ipaddress.IPv4Address("1.2.3.4")
    ip6 = ipaddress.IPv6Address("2001:db8::1")

    row = await ch_client.fetchone(
        """
        SELECT
            {bt:Bool} AS bt,
            {bf:Bool} AS bf,
            {u8:UInt8} AS u8,
            {i32:Int32} AS i32,
            {f64:Float64} AS f64,
            {s:String} AS s,
            {bs:String} AS bs,
            {d:Date} AS d,
            {dt:DateTime('UTC')} AS dt,
            {uid:UUID} AS uid,
            {ip4:IPv4} AS ip4,
            {ip6:IPv6} AS ip6,
            {dec:Decimal(10,2)} AS dec,
            {ns:Nullable(String)} AS ns,
            {ni:Nullable(Int32)} AS ni
        """,
        params={
            "bt": True,
            "bf": False,
            "u8": 5,
            "i32": -42,
            "f64": 3.5,
            "s": "hi",
            "bs": b"hello",
            "d": d,
            "dt": dt,
            "uid": uid,
            "ip4": ip4,
            "ip6": ip6,
            "dec": Decimal("123.45"),
            "ns": None,
            "ni": None,
        },
    )
    assert row is not None

    assert row["bt"] is True
    assert row["bf"] is False
    assert row["u8"] == 5
    assert row["i32"] == -42
    assert row["f64"] == pytest.approx(3.5)
    assert row["s"] == "hi"
    assert row["bs"] == "hello"
    assert row["d"] == d
    assert row["dt"] == dt
    assert row["uid"] == uid
    assert row["ip4"] == ip4
    assert row["ip6"] == ip6
    assert row["dec"] == Decimal("123.45")
    assert row["ns"] is None
    assert row["ni"] is None


async def test_params_container_types(ch_client: AsyncChClient):
    arr_s = ["a", "x\\y", "quote:'", "line\nbreak"]
    mp: dict[str, Any] = {"k": 1, "x\\y": -2, "quote:'": 3}
    mp_nested = {"a": [1, 2], "b": []}

    row = await ch_client.fetchone(
        """
        SELECT
            {arr_u8:Array(UInt8)} AS arr_u8,
            {arr_ns:Array(Nullable(String))} AS arr_ns,
            {arr_ni:Array(Nullable(Int32))} AS arr_ni,
            {arr_s:Array(String)} AS arr_s,
            {tup:Tuple(String, Int8)} AS tup,
            {tup_n:Tuple(String, Nullable(Int32))} AS tup_n,
            {mp:Map(String, Int32)} AS mp,
            {mp_n:Map(String, Nullable(Int32))} AS mp_n,
            {mp_arr_n:Map(String, Array(Nullable(Int32)))} AS mp_arr_n,
            {mp_nested:Map(String, Array(Int32))} AS mp_nested
        """,
        params={
            "arr_u8": [1, 2, 3],
            "arr_ns": [None, "x", None],
            "arr_ni": [None, 1, None, -2],
            "arr_s": arr_s,
            "tup": ("meta", 7),
            "tup_n": ("q", None),
            "mp": mp,
            "mp_n": {"a": None, "b": 2, "c": None},
            "mp_arr_n": {"a": [None, 1, None], "b": []},
            "mp_nested": mp_nested,
        },
    )
    assert row is not None
    assert row["arr_u8"] == [1, 2, 3]
    assert row["arr_ns"] == [None, "x", None]
    assert row["arr_ni"] == [None, 1, None, -2]
    assert row["arr_s"] == arr_s
    assert row["tup"] == ("meta", 7)
    assert row["tup_n"] == ("q", None)
    assert row["mp"] == mp
    assert row["mp_n"] == {"a": None, "b": 2, "c": None}
    assert row["mp_arr_n"] == {"a": [None, 1, None], "b": []}
    assert row["mp_nested"] == mp_nested
