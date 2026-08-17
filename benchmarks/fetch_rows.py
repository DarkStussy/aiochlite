"""
IO benchmark comparing ClickHouse HTTP clients under identical server-side data generation.

Measures end-to-end fetch+decode cost for a fixed query (no client-side insert).

Environment variables:
- CLICKHOUSE_HOST (default: localhost)
- CLICKHOUSE_PORT (default: 8123)
- CLICKHOUSE_USER (default: default)
- CLICKHOUSE_PASSWORD (default: empty)
- CLICKHOUSE_DATABASE (default: default)
- BENCH_ROWS (default: 100000)
- BENCH_ROUNDS (default: 5)
- BENCH_WARMUP (default: 2)
"""

import asyncio
import gc
import os
import sys
import time
from collections.abc import Callable, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from importlib.metadata import version
from typing import Any, AsyncIterator, NamedTuple, Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

import aiochclient
import clickhouse_connect
from aiohttp import ClientSession

import aiochlite

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", default="localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", default="8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", default="default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", default="")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", default="default")
BENCH_ROWS = int(os.getenv("BENCH_ROWS", default="100000"))
BENCH_ROUNDS = int(os.getenv("BENCH_ROUNDS", default="5"))
BENCH_WARMUP = int(os.getenv("BENCH_WARMUP", default="2"))


def _get_url():
    scheme = "https" if CLICKHOUSE_PORT == 8443 else "http"
    return f"{scheme}://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"


class _RowLike(Protocol):
    def __getitem__(self, key: str) -> Any: ...


def _epoch_seconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("UTC"))
    return int(value.timestamp())


def _flat_checksum(rows: Sequence[_RowLike]) -> int:
    total = 0
    for row in rows:
        total += int(row["id"])
        total += int(row["payload"][1])
        total += _epoch_seconds(row["event_time"])
        total += int(row["prices"][0] * 100)
    return total


def _flat_checksum_tuples(rows: Sequence[Sequence[Any]]) -> int:
    total = 0
    for id_, event_time, payload, prices in rows:
        total += int(id_)
        total += int(payload[1])
        total += _epoch_seconds(event_time)
        total += int(prices[0] * 100)
    return total


_TEXT_COLUMNS = tuple(f"s{index}" for index in range(9))


def _text_checksum(rows: Sequence[_RowLike]) -> int:
    total = 0
    for row in rows:
        total += int(row["id"])
        total += sum(len(row[name]) for name in _TEXT_COLUMNS)
    return total


def _text_checksum_tuples(rows: Sequence[Sequence[Any]]) -> int:
    total = 0
    for row in rows:
        total += int(row[0])
        total += sum(len(value) for value in row[1:])
    return total


def _nested_checksum(rows: Sequence[_RowLike]) -> int:
    total = 0
    for row in rows:
        total += int(row["id"])
        total += len(row["nested"])
        total += len(row["tags"])
        total += sum(value for value in row["opt"] if value is not None)
    return total


def _nested_checksum_tuples(rows: Sequence[Sequence[Any]]) -> int:
    total = 0
    for id_, nested, tags, opt in rows:
        total += int(id_)
        total += len(nested)
        total += len(tags)
        total += sum(value for value in opt if value is not None)
    return total


class Schema(NamedTuple):
    """A table shape, the data to fill it with, and how to reduce a row of it to a number.

    Every client decodes the same bytes, so the checksum only has to touch each column once; it is
    there to stop a client from returning rows it never decoded.
    """

    label: str
    columns: str
    generate: str
    select: str
    checksum: Callable[[Sequence[_RowLike]], int]
    checksum_tuples: Callable[[Sequence[Sequence[Any]]], int]


SCHEMAS: tuple[Schema, ...] = (
    Schema(
        label="flat columns",
        columns="""
            id UInt64,
            event_time DateTime('UTC'),
            payload Tuple(String, UInt16),
            prices Array(Decimal(10, 2))
        """,
        generate="""
            number as id,
            toDateTime(1734160800 + number, 'UTC') as event_time,
            tuple('evt', toUInt16(number % 65535)) as payload,
            [
                toDecimal64((number % 1000) / 100, 2),
                toDecimal64(((number + 1) % 1000) / 100, 2),
                toDecimal64(((number + 2) % 1000) / 100, 2)
            ] as prices
        """,
        select="id, event_time, payload, prices",
        checksum=_flat_checksum,
        checksum_tuples=_flat_checksum_tuples,
    ),
    Schema(
        label="wide strings",
        columns="id UInt64, " + ", ".join(f"{name} String" for name in _TEXT_COLUMNS),
        generate="number as id, "
        + ", ".join(
            f"concat('v{index}-', toString(number % 500)) as {name}" for index, name in enumerate(_TEXT_COLUMNS)
        ),
        select="id, " + ", ".join(_TEXT_COLUMNS),
        checksum=_text_checksum,
        checksum_tuples=_text_checksum_tuples,
    ),
    Schema(
        label="nested containers",
        columns="""
            id UInt64,
            nested Array(Array(UInt8)),
            tags Map(String, Array(UInt8)),
            opt Array(Nullable(UInt64))
        """,
        generate="""
            number as id,
            arrayMap(x -> range(x % 3), range(3)) as nested,
            map('a', [toUInt8(number % 255)], 'b', [toUInt8(1), toUInt8(2)]) as tags,
            arrayMap(x -> if(x % 2, NULL, toUInt64(x + number)), range(3)) as opt
        """,
        select="id, nested, tags, opt",
        checksum=_nested_checksum,
        checksum_tuples=_nested_checksum_tuples,
    ),
)


async def _setup_table(client: aiochlite.AsyncChClient, schema: Schema, table: str) -> None:
    await client.execute(f"CREATE TABLE {table} ({schema.columns}) ENGINE = Memory")
    # Server-side generation: avoids client-side insert overhead and guarantees identical data.
    await client.execute(f"INSERT INTO {table} SELECT {schema.generate} FROM numbers({BENCH_ROWS})")


def _print_rounds(label: str, rows: int, durations: list[float]) -> None:
    print(f"\nIO benchmark ({label})")
    for idx, dur in enumerate(durations, start=1):
        print(f"Round {idx}: {dur * 1000:8.2f} ms ({rows / dur:,.0f} rows/s, {(dur / rows) * 1e6:,.1f} µs/row)")
    if durations:
        avg = sum(durations) / len(durations)
        print(f"Avg:     {avg * 1000:8.2f} ms ({rows / avg:,.0f} rows/s, {(avg / rows) * 1e6:,.1f} µs/row)")


async def _bench_aiochlite_rows(schema: Schema, table: str) -> None:
    client = aiochlite.AsyncChClient(url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    try:
        query = f"SELECT {schema.select} FROM {table} ORDER BY id"
        for _ in range(BENCH_WARMUP):
            schema.checksum(await client.fetch(query))

        durations: list[float] = []
        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            chk = schema.checksum(await client.fetch(query))
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)
    finally:
        await client.close()

    _print_rounds("aiochlite (Row)", BENCH_ROWS, durations)


async def _bench_aiochlite_tuples(schema: Schema, table: str) -> None:
    client = aiochlite.AsyncChClient(url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    try:
        query = f"SELECT {schema.select} FROM {table} ORDER BY id"
        for _ in range(BENCH_WARMUP):
            schema.checksum_tuples(await client.fetch_rows(query))

        durations: list[float] = []
        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            chk = schema.checksum_tuples(await client.fetch_rows(query))
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)
    finally:
        await client.close()

    _print_rounds("aiochlite (tuples)", BENCH_ROWS, durations)


async def _bench_aiochclient(schema: Schema, table: str) -> None:
    query = f"SELECT {schema.select} FROM {table} ORDER BY id"

    durations: list[float] = []
    async with ClientSession() as session:
        client = aiochclient.ChClient(session, url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

        for _ in range(BENCH_WARMUP):
            schema.checksum(await client.fetch(query))

        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            chk = schema.checksum(await client.fetch(query))
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)

    _print_rounds("aiochclient", BENCH_ROWS, durations)


async def _bench_clickhouse_connect(schema: Schema, table: str) -> None:
    client = await clickhouse_connect.get_async_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        secure=CLICKHOUSE_PORT == 8443,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    query = f"SELECT {schema.select} FROM {table} ORDER BY id"

    try:
        for _ in range(BENCH_WARMUP):
            result = await client.query(query)
            schema.checksum_tuples(result.result_rows)

        durations: list[float] = []
        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            result = await client.query(query)
            chk = schema.checksum_tuples(result.result_rows)
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)
    finally:
        await client.close()

    _print_rounds("clickhouse-connect (async)", BENCH_ROWS, durations)


async def _print_environment(client: aiochlite.AsyncChClient) -> None:
    clients = ", ".join(f"{name} {version(name)}" for name in ("aiochlite", "aiochclient", "clickhouse-connect"))
    python_version = ".".join(str(part) for part in sys.version_info[:3])
    server_version: str = await client.fetchval("SELECT version()")
    print(f"Clients: {clients}")
    print(f"Python: {python_version}, ClickHouse: {server_version}")


@asynccontextmanager
async def create_table(schema: Schema) -> AsyncIterator[str]:
    client = aiochlite.AsyncChClient(url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    table = f"bench_io_{uuid4().hex}"
    columns = " ".join(schema.columns.split())
    print(f"\n=== Schema: {schema.label} — {columns}")
    print(f"Table: {table}")
    try:
        await _setup_table(client, schema, table)
        yield table
        await client.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        await client.close()


async def main() -> None:
    client = aiochlite.AsyncChClient(url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    try:
        await _print_environment(client)
        print(f"Rows: {BENCH_ROWS}, rounds: {BENCH_ROUNDS}, warmup: {BENCH_WARMUP}")
    finally:
        await client.close()

    for schema in SCHEMAS:
        async with create_table(schema) as table:
            await _bench_clickhouse_connect(schema, table)
            await _bench_aiochlite_rows(schema, table)
            await _bench_aiochlite_tuples(schema, table)
            await _bench_aiochclient(schema, table)


if __name__ == "__main__":
    asyncio.run(main())
