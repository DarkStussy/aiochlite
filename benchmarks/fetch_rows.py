"""
IO benchmark comparing ClickHouse HTTP clients under identical server-side data generation.

Measures end-to-end fetch+decode cost for a fixed query (no client-side insert).

Environment variables:
- CLICKHOUSE_HOST (default: localhost)
- CLICKHOUSE_PORT (default: 8123)
- CLICKHOUSE_USER (default: default)
- CLICKHOUSE_PASSWORD (default: empty)
- CLICKHOUSE_DATABASE (default: default)
- BENCH_ROWS (default: 10000)
- BENCH_ROUNDS (default: 3)
- BENCH_WARMUP (default: 1)
"""

import asyncio
import gc
import os
import time
from collections.abc import Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Protocol
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
BENCH_ROWS = int(os.getenv("BENCH_ROWS", default="10000"))
BENCH_ROUNDS = int(os.getenv("BENCH_ROUNDS", default="3"))
BENCH_WARMUP = int(os.getenv("BENCH_WARMUP", default="1"))


def _get_url():
    scheme = "https" if CLICKHOUSE_PORT == 8443 else "http"
    return f"{scheme}://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"


async def _setup_table(client: aiochlite.AsyncChClient, table: str) -> None:
    await client.execute(
        f"""
        CREATE TABLE {table} (
            id UInt64,
            event_time DateTime('UTC'),
            payload Tuple(String, UInt16),
            prices Array(Decimal(10, 2))
        ) ENGINE = Memory
        """
    )

    # Server-side generation: avoids client-side insert overhead and guarantees identical data.
    await client.execute(
        f"""
        INSERT INTO {table}
        SELECT
            number as id,
            toDateTime(1734160800 + number, 'UTC') as event_time,
            tuple('evt', toUInt16(number % 65535)) as payload,
            [
                toDecimal64((number % 1000) / 100, 2),
                toDecimal64(((number + 1) % 1000) / 100, 2),
                toDecimal64(((number + 2) % 1000) / 100, 2)
            ] as prices
        FROM numbers({BENCH_ROWS})
        """
    )


class _RowLike(Protocol):
    def __getitem__(self, key: str) -> Any: ...


def _epoch_seconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("UTC"))
    return int(value.timestamp())


def _checksum(rows: Sequence[_RowLike]) -> int:
    total = 0
    for row in rows:
        total += int(row["id"])
        total += int(row["payload"][1])
        total += _epoch_seconds(row["event_time"])
        total += int(row["prices"][0] * 100)
    return total


def _checksum_tuple_rows(rows: Sequence[Sequence[Any]]) -> int:
    total = 0
    for id_, event_time, payload, prices in rows:
        total += int(id_)
        total += int(payload[1])
        total += _epoch_seconds(event_time)
        total += int(prices[0] * 100)
    return total


def _print_rounds(label: str, rows: int, durations: list[float]) -> None:
    print(f"\nIO benchmark ({label})")
    for idx, dur in enumerate(durations, start=1):
        print(f"Round {idx}: {dur * 1000:8.2f} ms ({rows / dur:,.0f} rows/s, {(dur / rows) * 1e6:,.1f} µs/row)")
    if durations:
        avg = sum(durations) / len(durations)
        print(f"Avg:     {avg * 1000:8.2f} ms ({rows / avg:,.0f} rows/s, {(avg / rows) * 1e6:,.1f} µs/row)")


async def _bench_aiochlite_rows(table: str) -> None:
    client = aiochlite.AsyncChClient(
        url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, lazy_decode=False
    )
    try:
        query = f"SELECT id, event_time, payload, prices FROM {table} ORDER BY id"
        for _ in range(BENCH_WARMUP):
            _checksum(await client.fetch(query))

        durations: list[float] = []
        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            chk = _checksum(await client.fetch(query))
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)
    finally:
        await client.close()

    _print_rounds("aiochlite (Row)", BENCH_ROWS, durations)


async def _bench_aiochlite_tuples(table: str) -> None:
    client = aiochlite.AsyncChClient(
        url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, lazy_decode=False
    )
    try:
        query = f"SELECT id, event_time, payload, prices FROM {table} ORDER BY id"
        for _ in range(BENCH_WARMUP):
            _checksum_tuple_rows(await client.fetch_rows(query))

        durations: list[float] = []
        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            chk = _checksum_tuple_rows(await client.fetch_rows(query))
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)
    finally:
        await client.close()

    _print_rounds("aiochlite (tuples)", BENCH_ROWS, durations)


async def _bench_aiochclient(table: str) -> None:
    query = f"SELECT id, event_time, payload, prices FROM {table} ORDER BY id"

    durations: list[float] = []
    async with ClientSession() as session:
        client = aiochclient.ChClient(session, url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

        for _ in range(BENCH_WARMUP):
            _checksum(await client.fetch(query))

        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            chk = _checksum(await client.fetch(query))
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)

    _print_rounds("aiochclient", BENCH_ROWS, durations)


async def _bench_clickhouse_connect(table: str) -> None:
    client = await clickhouse_connect.get_async_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        secure=CLICKHOUSE_PORT == 8443,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    query = f"SELECT id, event_time, payload, prices FROM {table} ORDER BY id"

    try:
        for _ in range(BENCH_WARMUP):
            result = await client.query(query)
            _checksum_tuple_rows(result.result_rows)

        durations: list[float] = []
        for _ in range(BENCH_ROUNDS):
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            result = await client.query(query)
            chk = _checksum_tuple_rows(result.result_rows)
            dur = time.perf_counter() - t0
            gc.enable()
            if chk == -1:
                raise RuntimeError("Impossible checksum")
            durations.append(dur)
    finally:
        await client.close()

    _print_rounds("clickhouse-connect (async)", BENCH_ROWS, durations)


@asynccontextmanager
async def create_table() -> AsyncIterator[str]:
    client = aiochlite.AsyncChClient(url=_get_url(), user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    table = f"bench_io_{uuid4().hex}"
    print(f"Rows: {BENCH_ROWS}, rounds: {BENCH_ROUNDS}, warmup: {BENCH_WARMUP}")
    print(f"Table: {table}")
    try:
        await _setup_table(client, table)
        yield table
        await client.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        await client.close()


async def main() -> None:
    async with create_table() as table:
        await _bench_clickhouse_connect(table)
        await _bench_aiochlite_rows(table)
        await _bench_aiochlite_tuples(table)
        await _bench_aiochclient(table)


if __name__ == "__main__":
    asyncio.run(main())
