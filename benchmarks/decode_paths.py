"""Decoder benchmark: one reader call per column against the path a query takes now.

Isolates row decoding from everything around it. The payload is fetched once, before any timing
starts, so neither the network nor the server appears in the measurement — only the decoding of
bytes already in memory.

The slow side keeps one reader call per column per row, which is what a schema falls back to when
nothing in it can be emitted inline. The fast side is whatever `parse_rowbinary_with_names_and_types`
would pick for the same types: one `struct` pass over the whole body for a row that is fixed-width
end to end, otherwise a loop compiled for the schema.

Methodology:
- The two paths are checked for identical output once, outside the timer.
- Both are rebuilt every round with the module caches cleared, so a converter cache left warm by the
  round before cannot be mistaken for a fast decode.
- Rounds alternate which path runs first, so drift over the run affects both.
- Each decoded result is released before the next measurement starts.
- Medians are reported alongside the raw series, since microbenchmarks are noisy.
- The last section sweeps row width at parity: N consecutive `UInt64` columns for N = 2..10.

Environment variables:
- CLICKHOUSE_HOST (default: localhost)
- CLICKHOUSE_PORT (default: 8123)
- CLICKHOUSE_USER (default: default)
- CLICKHOUSE_PASSWORD (default: empty)
- CLICKHOUSE_DATABASE (default: default)
- BENCH_ROWS (default: 200000)
- BENCH_ROUNDS (default: 7)
- BENCH_WARMUP (default: 2)
- BENCH_SWEEP_ROWS (default: 100000)
"""

import gc
import os
import platform
import statistics
import subprocess
import time
from importlib.metadata import version
from pathlib import Path
from typing import Any, Callable, NamedTuple

import aiochlite
from aiochlite.converters import rowbinary
from aiochlite.converters.rowbinary import (
    _BinaryReader,
    _bulk_decode,
    _fixed_row_layout,
    _reader_for_type,
    _row_decoder,
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", default="localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", default="8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", default="default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", default="")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", default="default")
BENCH_ROWS = int(os.getenv("BENCH_ROWS", default="200000"))
BENCH_ROUNDS = int(os.getenv("BENCH_ROUNDS", default="7"))
BENCH_WARMUP = int(os.getenv("BENCH_WARMUP", default="2"))
BENCH_SWEEP_ROWS = int(os.getenv("BENCH_SWEEP_ROWS", default="100000"))

# These payloads carry no timezone header of their own, so the decoder is given one explicitly.
SERVER_TZ = "UTC"

_Decoder = Callable[[bytes, int], list[Any]]


class Schema(NamedTuple):
    """One benchmark case: a column layout and the query that produces it."""

    label: str
    types: tuple[str, ...]
    query: str


SCHEMAS: tuple[Schema, ...] = (
    Schema(
        "Fully fixed-width row (5 columns)",
        ("UInt64", "UInt32", "Float64", "Int16", "DateTime('UTC')"),
        """SELECT number::UInt64, (number % 1000)::UInt32, (number / 7)::Float64,
                  (number % 100)::Int16, toDateTime(1734160800 + number, 'UTC')
            FROM numbers({rows})""",
    ),
    Schema(
        "Mixed row, one run of 3 (5 columns)",
        ("UInt64", "String", "DateTime('UTC')", "Float64", "Int32"),
        """SELECT number::UInt64, concat('evt', toString(number % 1000)),
                  toDateTime(1734160800 + number, 'UTC'), (number / 7)::Float64,
                  (number % 100)::Int32
            FROM numbers({rows})""",
    ),
    Schema(
        "Wide numeric row (10 columns)",
        ("UInt64",) * 5 + ("Float64",) * 5,
        """SELECT number::UInt64, (number + 1)::UInt64, (number + 2)::UInt64, (number + 3)::UInt64,
                  (number + 4)::UInt64, (number / 7)::Float64, (number / 11)::Float64,
                  (number / 13)::Float64, (number / 17)::Float64, (number / 19)::Float64
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


def _read_header(data: bytes) -> tuple[list[str], int]:
    """Read the header, returning the column types and the offset of the first row."""
    reader = _BinaryReader(data)
    count = reader.read_varuint()
    for _ in range(count):
        reader.read_string()
    types = [reader.read_string() for _ in range(count)]
    return types, reader.pos


def _per_field_decoder(types: list[str]) -> _Decoder:
    """One reader call per column per row, the path a schema takes when nothing compiles."""
    readers = [_reader_for_type(ch_type, SERVER_TZ) for ch_type in types]

    def decode(data: bytes, body: int) -> list[Any]:
        reader = _BinaryReader(data)
        reader.pos = body
        rows: list[Any] = []
        while not reader.eof:
            rows.append([read(reader) for read in readers])

        return rows

    return decode


def _current_decoder(types: list[str]) -> tuple[_Decoder, str]:
    """The path a query takes for these types, and the name of it."""
    layout = _fixed_row_layout(types, SERVER_TZ)
    if layout is not None:

        def bulk(data: bytes, body: int) -> list[Any]:
            return _bulk_decode(data, body, *layout, as_tuple=False)

        return bulk, "one struct pass over the body"

    compiled = _row_decoder(types, SERVER_TZ, as_tuple=False)
    if compiled is None:
        raise RuntimeError(f"{types} does not compile; the two paths would be the same")

    def run(data: bytes, body: int) -> list[Any]:
        rows, pos = compiled(data, body, len(data))
        if pos != len(data):
            raise RuntimeError("Decoder stopped short of the payload")

        return rows

    return run, "loop compiled for the schema"


def _build(types: list[str]) -> tuple[_Decoder, _Decoder, str]:
    """Both decoders, freshly built, with whatever the last round cached thrown away."""
    rowbinary._reader_for_type.cache_clear()
    rowbinary._compiled_row_decoder.cache_clear()
    current, path = _current_decoder(types)
    return _per_field_decoder(types), current, path


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


def _measure(types: list[str], data: bytes, body: int, rows: int) -> tuple[list[float], list[float]]:
    """Time both paths, alternating which one runs first."""
    for _ in range(BENCH_WARMUP):
        per_field, current, _ = _build(types)
        _time_once(per_field, data, body, rows)
        _time_once(current, data, body, rows)

    per_field_times: list[float] = []
    current_times: list[float] = []
    for round_index in range(BENCH_ROUNDS):
        per_field, current, _ = _build(types)
        if round_index % 2:
            current_times.append(_time_once(current, data, body, rows))
            per_field_times.append(_time_once(per_field, data, body, rows))
        else:
            per_field_times.append(_time_once(per_field, data, body, rows))
            current_times.append(_time_once(current, data, body, rows))

    return per_field_times, current_times


def _series(durations: list[float]) -> str:
    return ", ".join(f"{value * 1000:.2f}" for value in durations)


def _report(label: str, per_field: list[float], current: list[float]) -> None:
    slow = statistics.median(per_field)
    fast = statistics.median(current)
    print(f"\n{label}")
    print(f"  Per field: median {slow * 1000:7.2f} ms  [{_series(per_field)}]")
    print(f"  Current:   median {fast * 1000:7.2f} ms  [{_series(current)}]")
    print(f"  Speedup:   {slow / fast:.2f}x")


async def _payload(client: aiochlite.AsyncChClient, query: str) -> tuple[bytes, list[str], int]:
    """Fetch one payload and read its header, so timing sees only bytes already in memory."""
    data = await client.fetch_format(query, "RowBinaryWithNamesAndTypes")
    types, body = _read_header(data)
    return data, types, body


def _check_agreement(types: list[str], data: bytes, body: int) -> str:
    """Compare the two paths outside the timer, returning the name of the fast one."""
    per_field, current, path = _build(types)
    if per_field(data, body) != current(data, body):
        raise RuntimeError(f"The two paths disagree on {types}")

    return path


def _print_environment() -> None:
    print(f"aiochlite {version('aiochlite')} @ {_commit_sha()}")
    print(f"CPU: {_cpu_model()}")
    print(f"OS: {platform.platform()}")
    print(f"Python: {platform.python_version()} ({platform.python_implementation()})")
    print(f"Rows: {BENCH_ROWS}, rounds: {BENCH_ROUNDS}, warmup: {BENCH_WARMUP}")
    print(f"Sweep rows: {BENCH_SWEEP_ROWS}")


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

        for schema in SCHEMAS:
            data, types, body = await _payload(client, schema.query.format(rows=BENCH_ROWS))
            if tuple(types) != schema.types:
                raise RuntimeError(f"Server returned {types}, expected {list(schema.types)}")

            path = _check_agreement(types, data, body)
            per_field, current = _measure(types, data, body, BENCH_ROWS)
            _report(f"{schema.label} — {path}", per_field, current)

        print(f"\nRow width at parity, {BENCH_SWEEP_ROWS} rows of UInt64 columns")
        for width in range(2, 11):
            columns = ", ".join(f"(number + {index})::UInt64" for index in range(width))
            data, types, body = await _payload(client, f"SELECT {columns} FROM numbers({BENCH_SWEEP_ROWS})")
            _check_agreement(types, data, body)
            per_field, current = _measure(types, data, body, BENCH_SWEEP_ROWS)
            _report(f"{width} columns", per_field, current)
    finally:
        await client.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
