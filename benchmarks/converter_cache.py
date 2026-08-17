"""Converter benchmark: what the value cache costs and what it buys.

Fixed-width columns such as `DateTime` and `Decimal` arrive as plain integers and are turned into
Python objects by a converter. Converters built per query memoize through `_value_cache`, which pays
off only when values repeat. This script measures both sides of that trade on the same decoder.

Two payloads with the same schema are compared:
- low cardinality: a few hundred distinct timestamps and prices, as in rounded event time;
- high cardinality: every value distinct, as in monotonic event time.

Each payload is decoded twice, by a row reader whose converters memoize and by one whose converters
do not. The uncached variant is built by neutralizing `_value_cache` while the converters are
created, so both readers run identical conversion logic.

Environment variables:
- CLICKHOUSE_HOST (default: localhost)
- CLICKHOUSE_PORT (default: 8123)
- CLICKHOUSE_USER (default: default)
- CLICKHOUSE_PASSWORD (default: empty)
- CLICKHOUSE_DATABASE (default: default)
- BENCH_ROWS (default: 200000)
- BENCH_ROUNDS (default: 7)
- BENCH_WARMUP (default: 2)
"""

import gc
import os
import platform
import statistics
import subprocess
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path
from typing import Any, NamedTuple

import aiochlite
from aiochlite.converters import rowbinary
from aiochlite.converters.rowbinary import _BinaryReader, _make_row_reader

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", default="localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", default="8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", default="default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", default="")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", default="default")
BENCH_ROWS = int(os.getenv("BENCH_ROWS", default="200000"))
BENCH_ROUNDS = int(os.getenv("BENCH_ROUNDS", default="7"))
BENCH_WARMUP = int(os.getenv("BENCH_WARMUP", default="2"))

SERVER_TZ = "UTC"

# One UInt64 plus the two cached converter kinds, all fixed-width so the row fuses into one struct
# and the converters are the only difference left between the two readers.
COLUMN_TYPES: tuple[str, ...] = ("UInt64", "DateTime('UTC')", "Decimal(18, 2)")

_Decoder = Callable[[bytes, int], list[Any]]


class Case(NamedTuple):
    """One cardinality regime and the query that produces it."""

    label: str
    detail: str
    query: str


CASES: tuple[Case, ...] = (
    Case(
        "Low cardinality",
        "200 distinct timestamps, 100 distinct prices",
        """SELECT number::UInt64,
                  toDateTime(1734160800 + intDiv(number, 1000), 'UTC'),
                  toDecimal64((number % 100) / 100, 2)
            FROM numbers({rows})""",
    ),
    Case(
        "High cardinality",
        "every timestamp and price distinct",
        """SELECT number::UInt64,
                  toDateTime(1734160800 + number, 'UTC'),
                  toDecimal64(number / 100, 2)
            FROM numbers({rows})""",
    ),
)


def _get_url() -> str:
    scheme = "https" if CLICKHOUSE_PORT == 8443 else "http"
    return f"{scheme}://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"


def _cpu_model() -> str:
    try:
        with Path("/proc/cpuinfo").open(encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def _commit_sha() -> str:
    try:
        completed = subprocess.run(
            ["/usr/bin/env", "git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _no_cache(convert: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Stand-in for `_value_cache` that memoizes nothing."""
    return convert


@contextmanager
def _converters_without_cache() -> Iterator[None]:
    """Build converters without memoization for the duration of the block.

    `_fixed_field` looks up `_value_cache` at call time, so replacing the name is enough.
    `_reader_for_type` caches whole readers, so its cache is cleared on both edges.
    """
    original = rowbinary._value_cache
    setattr(rowbinary, "_value_cache", _no_cache)
    rowbinary._reader_for_type.cache_clear()
    try:
        yield
    finally:
        setattr(rowbinary, "_value_cache", original)
        rowbinary._reader_for_type.cache_clear()


def _memoizes(ch_type: str) -> bool:
    """Whether the converter built for this type memoizes."""
    field = rowbinary._fixed_field(ch_type, SERVER_TZ)
    if field is None:
        raise RuntimeError(f"{ch_type} is not a fixed-width column")

    return isinstance(getattr(field[1], "__self__", None), rowbinary._ValueCache)


def _build_readers() -> tuple[_Decoder, _Decoder]:
    """Return decoders for the same schema, one with memoizing converters and one without."""
    types = list(COLUMN_TYPES)

    with _converters_without_cache():
        if _memoizes("Decimal(18, 2)"):
            raise RuntimeError("Converters are still memoizing; the benchmark would compare nothing")
        uncached_row = _make_row_reader(types, SERVER_TZ)

    if not _memoizes("Decimal(18, 2)"):
        raise RuntimeError("Converters lost their cache outside the patch")
    cached_row = _make_row_reader(types, SERVER_TZ)

    if uncached_row is None or cached_row is None:
        raise RuntimeError("Schema did not fuse; the converters would not be exercised the same way")

    def make(read_row: Callable[[Any], list[Any]]) -> _Decoder:
        def decode(data: bytes, body: int) -> list[Any]:
            reader = _BinaryReader(data)
            reader.pos = body
            rows: list[Any] = []
            while not reader.eof:
                rows.append(read_row(reader))
            return rows

        return decode

    return make(cached_row), make(uncached_row)


def _read_header(data: bytes) -> tuple[list[str], int]:
    """Read the header, returning the column types and the offset of the first row."""
    reader = _BinaryReader(data)
    count = reader.read_varuint()
    for _ in range(count):
        reader.read_string()
    types = [reader.read_string() for _ in range(count)]
    return types, reader.pos


def _time_once(decode: _Decoder, data: bytes, body: int, expected_rows: int) -> float:
    """Time one full decode, releasing the result before returning."""
    gc.collect()
    gc.disable()
    start = time.perf_counter()
    rows = decode(data, body)
    duration = time.perf_counter() - start
    gc.enable()

    count = len(rows)
    del rows
    if count != expected_rows:
        raise RuntimeError(f"Decoded {count} rows, expected {expected_rows}")

    return duration


def _measure(
    cached: _Decoder, uncached: _Decoder, data: bytes, body: int, rows: int
) -> tuple[list[float], list[float]]:
    """Time both decoders, alternating which one runs first."""
    for _ in range(BENCH_WARMUP):
        _time_once(cached, data, body, rows)
        _time_once(uncached, data, body, rows)

    cached_times: list[float] = []
    uncached_times: list[float] = []
    for round_index in range(BENCH_ROUNDS):
        if round_index % 2:
            uncached_times.append(_time_once(uncached, data, body, rows))
            cached_times.append(_time_once(cached, data, body, rows))
        else:
            cached_times.append(_time_once(cached, data, body, rows))
            uncached_times.append(_time_once(uncached, data, body, rows))

    return cached_times, uncached_times


def _series(durations: list[float]) -> str:
    return ", ".join(f"{value * 1000:.2f}" for value in durations)


def _print_environment() -> None:
    print(f"aiochlite {version('aiochlite')} @ {_commit_sha()}")
    print(f"CPU: {_cpu_model()}")
    print(f"OS: {platform.platform()}")
    print(f"Python: {platform.python_version()} ({platform.python_implementation()})")
    print(f"Schema: {', '.join(COLUMN_TYPES)}")
    print(f"Rows: {BENCH_ROWS}, rounds: {BENCH_ROUNDS}, warmup: {BENCH_WARMUP}")


async def main() -> None:
    client = aiochlite.AsyncChClient(
        url=_get_url(),
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    try:
        _print_environment()
        server_version: str = await client.fetchval("SELECT version()")
        print(f"ClickHouse: {server_version}")

        cached, uncached = _build_readers()

        for case in CASES:
            payload = await client.fetch_format(case.query.format(rows=BENCH_ROWS), "RowBinaryWithNamesAndTypes")
            types, body = _read_header(payload)
            if tuple(types) != COLUMN_TYPES:
                raise RuntimeError(f"Server returned {types}, expected {list(COLUMN_TYPES)}")

            if cached(payload, body) != uncached(payload, body):
                raise RuntimeError("Cached and uncached decoders disagree")

            cached_times, uncached_times = _measure(cached, uncached, payload, body, BENCH_ROWS)
            cached_median = statistics.median(cached_times)
            uncached_median = statistics.median(uncached_times)

            print(f"\n{case.label} — {case.detail}")
            print(f"  Cached:   median {cached_median * 1000:7.2f} ms  [{_series(cached_times)}]")
            print(f"  Uncached: median {uncached_median * 1000:7.2f} ms  [{_series(uncached_times)}]")
            print(f"  Cache changes decode time by {(cached_median / uncached_median - 1) * 100:+.1f}%")
    finally:
        await client.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
