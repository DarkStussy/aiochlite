from __future__ import annotations

import ipaddress
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient

from ._types import TableFactory

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


async def test_insert_tuple_rows(ch_client: AsyncChClient, make_table: TableFactory):
    data = [(1, "Alice"), (2, "Bob")]

    table = await make_table(id="UInt32", name="String")
    await ch_client.insert(table, data, column_names=("id", "name"))
    rows = await ch_client.fetch(f"SELECT id, name FROM {table} ORDER BY id")

    assert [(r["id"], r["name"]) for r in rows] == data


async def test_insert_dict_rows_scalar_types(ch_client: AsyncChClient, make_table: TableFactory):
    table = await make_table(
        id="UInt32",
        b="Bool",
        i32="Int32",
        u64="UInt64",
        f64="Float64",
        s="String",
        bs="String",
        d="Date",
        dt="DateTime('UTC')",
        dec="Decimal(10, 2)",
        uid="UUID",
        ip4="IPv4",
        ip6="IPv6",
        n_s="Nullable(String)",
    )

    uid = UUID("550e8400-e29b-41d4-a716-446655440000")
    dt = datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    d = date(2025, 12, 14)
    ip4 = ipaddress.IPv4Address("1.2.3.4")
    ip6 = ipaddress.IPv6Address("2001:db8::1")

    await ch_client.insert(
        table,
        [
            {
                "id": 1,
                "b": True,
                "i32": -42,
                "u64": 10_000_000_000,
                "f64": 3.5,
                "s": "quote:' and backslash:\\",
                "bs": b"hello",
                "d": d,
                "dt": dt,
                "dec": Decimal("123.45"),
                "uid": uid,
                "ip4": ip4,
                "ip6": ip6,
                "n_s": None,
            }
        ],
    )

    row = await ch_client.fetchone(f"SELECT * FROM {table}")
    assert row is not None
    assert row["id"] == 1
    assert row["b"] is True
    assert row["i32"] == -42
    assert row["u64"] == 10_000_000_000
    assert row["f64"] == pytest.approx(3.5)
    assert row["s"] == "quote:' and backslash:\\"
    assert row["bs"] == "hello"
    assert row["d"] == d
    assert row["dt"] == dt
    assert row["dec"] == Decimal("123.45")
    assert row["uid"] == uid
    assert row["ip4"] == ip4
    assert row["ip6"] == ip6
    assert row["n_s"] is None


async def test_insert_tuple_rows_complex_types(ch_client: AsyncChClient, make_table: TableFactory):
    table = await make_table(
        id="UInt32",
        arr_ns="Array(Nullable(String))",
        tup="Tuple(String, Int8)",
        mp_n="Map(String, Nullable(Int32))",
        mp_arr_n="Map(String, Array(Nullable(Int32)))",
    )

    await ch_client.insert(
        table,
        [
            (
                1,
                [None, "x", None],
                ("meta", 7),
                {"a": None, "b": 2, "c": None},
                {"a": [None, 1, None], "b": []},
            )
        ],
        column_names=("id", "arr_ns", "tup", "mp_n", "mp_arr_n"),
    )

    row = await ch_client.fetchone(f"SELECT * FROM {table}")
    assert row is not None
    assert row["id"] == 1
    assert row["arr_ns"] == [None, "x", None]
    assert row["tup"] == ("meta", 7)
    assert row["mp_n"] == {"a": None, "b": 2, "c": None}
    assert row["mp_arr_n"] == {"a": [None, 1, None], "b": []}


async def test_insert_json_type(ch_client: AsyncChClient, make_table: TableFactory):
    table = await make_table(id="UInt32", doc="JSON")

    doc = {"a": 1, "b": [True, None, {"c": "x"}]}
    await ch_client.insert(table, [{"id": 1, "doc": doc}])

    row = await ch_client.fetchone(f"SELECT id, doc FROM {table}")
    assert row is not None
    assert row["id"] == 1
    assert row["doc"] == doc


async def test_insert_datetime64(ch_client: AsyncChClient, make_table: TableFactory):
    table = await make_table(
        id="UInt32",
        ts_utc="DateTime64(6, 'UTC')",
        ts_msk="DateTime64(6, 'Europe/Moscow')",
    )

    await ch_client.execute(
        f"""
        INSERT INTO {table} VALUES
            (1, toDateTime64('2025-12-14 10:00:00.123456', 6, 'UTC'),
                toDateTime64('2025-12-14 13:30:45.123456', 6, 'Europe/Moscow'))
        """
    )

    row = await ch_client.fetchone(f"SELECT id, ts_utc, ts_msk FROM {table} WHERE id = 1")
    assert row is not None
    assert row["id"] == 1
    assert row["ts_utc"] == datetime(2025, 12, 14, 10, 0, 0, 123456, tzinfo=ZoneInfo("UTC"))
    assert row["ts_msk"] == datetime(2025, 12, 14, 13, 30, 45, 123456, tzinfo=ZoneInfo("Europe/Moscow"))
