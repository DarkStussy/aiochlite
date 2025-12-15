from __future__ import annotations

import ipaddress
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient
from aiochlite.core import ExternalTable

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


async def test_external_tables(ch_client: AsyncChClient):
    ext = ExternalTable(structure=(("id", "UInt32"), ("name", "String")), data=((1, "Alice"), (2, "Bob")))
    value = await ch_client.fetchval(
        "SELECT count() FROM ext WHERE id >= 2",
        external_tables={"ext": ext},
    )
    assert value == 1


async def test_external_tables_scalar_types(ch_client: AsyncChClient):
    uid = UUID("550e8400-e29b-41d4-a716-446655440000")
    dt = datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    doc = {"a": 1, "b": [True, None]}

    ext = ExternalTable(
        structure=(
            ("id", "UInt32"),
            ("b", "Bool"),
            ("i32", "Int32"),
            ("u64", "UInt64"),
            ("f64", "Float64"),
            ("s", "String"),
            ("d", "Date"),
            ("dt", "DateTime('UTC')"),
            ("dec", "Decimal(10, 2)"),
            ("uid", "UUID"),
            ("n_s", "Nullable(String)"),
            ("ip4", "IPv4"),
            ("ip6", "IPv6"),
            ("doc", "JSON"),
        ),
        data=(
            (
                1,
                True,
                -42,
                10_000_000_000,
                3.5,
                "hi",
                date(2025, 12, 14),
                dt,
                Decimal("123.45"),
                uid,
                None,
                ipaddress.IPv4Address("1.2.3.4"),
                ipaddress.IPv6Address("2001:db8::1"),
                doc,
            ),
        ),
    )

    row = await ch_client.fetchone("SELECT * FROM ext", external_tables={"ext": ext})
    assert row is not None
    assert row["id"] == 1
    assert row["b"] is True
    assert row["i32"] == -42
    assert row["u64"] == 10_000_000_000
    assert row["f64"] == pytest.approx(3.5)
    assert row["s"] == "hi"
    assert row["d"] == date(2025, 12, 14)
    assert row["dt"] == dt
    assert row["dec"] == Decimal("123.45")
    assert row["uid"] == uid
    assert row["n_s"] is None
    assert row["ip4"] == ipaddress.IPv4Address("1.2.3.4")
    assert row["ip6"] == ipaddress.IPv6Address("2001:db8::1")
    assert row["doc"] == doc


async def test_external_tables_container_types(ch_client: AsyncChClient):
    ext = ExternalTable(
        structure=(
            ("id", "UInt32"),
            ("arr_u8", "Array(UInt8)"),
            ("arr_ns", "Array(Nullable(String))"),
            ("tup", "Tuple(String, Int8)"),
            ("tup_n", "Tuple(Nullable(String), Nullable(Int32))"),
            ("mp", "Map(String, Int32)"),
            ("mp_n", "Map(String, Nullable(Int32))"),
            ("mp_arr_n", "Map(String, Array(Nullable(Int32)))"),
        ),
        data=(
            (
                1,
                [1, 2, 3],
                [None, "x", None],
                ("meta", 7),
                (None, None),
                {"a": 1, "b": -2},
                {"a": None, "b": 2, "c": None},
                {"a": [None, 1, None], "b": []},
            ),
        ),
    )

    row = await ch_client.fetchone("SELECT * FROM ext", external_tables={"ext": ext})
    assert row is not None
    assert row["id"] == 1
    assert row["arr_u8"] == [1, 2, 3]
    assert row["arr_ns"] == [None, "x", None]
    assert row["tup"] == ("meta", 7)
    assert row["tup_n"] == (None, None)
    assert row["mp"] == {"a": 1, "b": -2}
    assert row["mp_n"] == {"a": None, "b": 2, "c": None}
    assert row["mp_arr_n"] == {"a": [None, 1, None], "b": []}
