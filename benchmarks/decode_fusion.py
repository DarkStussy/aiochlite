"""Decoder benchmark: per-field vs fused decoding of the same RowBinaryWithNamesAndTypes payload.

Isolates the fixed-width fusion in the row decoder from everything around it. The payload is
fetched once, before any timing starts, so neither the network nor the server appears in the
measurement — only the decoding of bytes already in memory.

The fused decoder groups runs of consecutive fixed-width columns into a single `struct` call;
the per-field decoder keeps one reader call per column.

Methodology:
- The two decoders are checked for identical output once, outside the timer.
- Rounds alternate the order of the two decoders (ABBA), so drift over the run affects both.
- Each decoded result is released before the next measurement starts.
- Medians are reported alongside the raw series, since microbenchmarks are noisy.
- The last section sweeps run length at parity: N consecutive UInt64 columns for N = 2..10.

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
from aiochlite.converters.rowbinary import _BinaryReader, _make_row_reader, _reader_for_type

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


class Result(NamedTuple):
    """Timings for both decoders on one schema, in seconds."""

    per_field: list[float]
    fused: list[float]


SCHEMAS: tuple[Schema, ...] = (
    Schema(
        "Fully fixed-width row (5 columns)",
        ("UInt64", "UInt32", "Float64", "Int16", "DateTime('UTC')"),
        """SELECT number::UInt64, (number % 1000)::UInt32, (number / 7)::Float64,
                  (number % 100)::Int16, toDateTime(1734160800 + number, 'UTC')
            FROM numbers({rows})""",
    ),
    Schema(
        "Mixed row, one fusable run of 3 (5 columns)",
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
    """Read column names and types, returning the types and the offset of the first row.

    Args:
        data (bytes): RowBinaryWithNamesAndTypes payload.

    Returns:
        types: Column types as sent by the server.
        body: Offset at which row data begins.
    """
    reader = _BinaryReader(data)
    count = reader.read_varuint()
    for _ in range(count):
        reader.read_string()
    types = [reader.read_string() for _ in range(count)]
    return types, reader.pos


def _per_field_decoder(types: list[str]) -> _Decoder:
    """Build a decoder that calls one reader per column, the way an unfused decoder works."""
    readers = [_reader_for_type(tp, SERVER_TZ) for tp in types]

    def decode(data: bytes, body: int) -> list[Any]:
        reader = _BinaryReader(data)
        reader.pos = body
        rows: list[Any] = []
        while not reader.eof:
            rows.append([read(reader) for read in readers])
        return rows

    return decode


def _fused_decoder(types: list[str]) -> _Decoder | None:
    """Build the fused decoder, or None when no run in this schema is long enough to fuse."""
    read_row = _make_row_reader(types, SERVER_TZ)
    if read_row is None:
        return None

    def decode(data: bytes, body: int) -> list[Any]:
        reader = _BinaryReader(data)
        reader.pos = body
        rows: list[Any] = []
        while not reader.eof:
            rows.append(read_row(reader))
        return rows

    return decode


def _check_agreement(per_field: _Decoder, fused: _Decoder, data: bytes, body: int) -> int:
    """Decode both ways outside the timer and require identical rows.

    Returns:
        int: Number of decoded rows.
    """
    left = per_field(data, body)
    right = fused(data, body)
    if left != right:
        for index, (lhs, rhs) in enumerate(zip(left, right, strict=True)):
            if lhs != rhs:
                raise RuntimeError(f"Decoders disagree at row {index}: {lhs!r} != {rhs!r}")
        raise RuntimeError(f"Decoders disagree: {len(left)} rows against {len(right)}")
    return len(left)


def _time_once(decode: _Decoder, data: bytes, body: int, expected_rows: int) -> float:
    """Time one full decode, releasing the result before returning so it cannot outlive the round."""
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


def _measure(per_field: _Decoder, fused: _Decoder, data: bytes, body: int, expected_rows: int) -> Result:
    """Time both decoders, alternating which one runs first so drift is shared evenly."""
    for _ in range(BENCH_WARMUP):
        _time_once(per_field, data, body, expected_rows)
        _time_once(fused, data, body, expected_rows)

    result = Result([], [])
    for round_index in range(BENCH_ROUNDS):
        if round_index % 2:
            result.fused.append(_time_once(fused, data, body, expected_rows))
            result.per_field.append(_time_once(per_field, data, body, expected_rows))
        else:
            result.per_field.append(_time_once(per_field, data, body, expected_rows))
            result.fused.append(_time_once(fused, data, body, expected_rows))

    return result


def _series(durations: list[float]) -> str:
    return ", ".join(f"{value * 1000:.2f}" for value in durations)


def _print_case(schema: Schema, payload_size: int, rows: int, result: Result) -> None:
    per_median = statistics.median(result.per_field)
    fused_median = statistics.median(result.fused)

    print(f"\n{schema.label}")
    print(f"  Schema:    {', '.join(schema.types)}")
    print(f"  Payload:   {payload_size / 1024 / 1024:.1f} MiB, {rows:,} rows")
    print(f"  Per-field: median {per_median * 1000:7.2f} ms  [{_series(result.per_field)}]")
    print(f"  Fused:     median {fused_median * 1000:7.2f} ms  [{_series(result.fused)}]")
    print(f"  Ratio:     {per_median / fused_median:.2f}x ({(fused_median / per_median - 1) * 100:+.1f}% time)")


def _print_environment() -> None:
    print(f"aiochlite {version('aiochlite')} @ {_commit_sha()}")
    print(f"CPU: {_cpu_model()}")
    print(f"OS: {platform.platform()}")
    print(f"Python: {platform.python_version()} ({platform.python_implementation()})")
    print(f"Rounds: {BENCH_ROUNDS}, warmup: {BENCH_WARMUP}")


def _sweep_schema(width: int) -> Schema:
    columns = ", ".join(f"(number + {index})::UInt64" for index in range(width))
    return Schema(
        f"{width} consecutive UInt64 columns",
        ("UInt64",) * width,
        f"SELECT {columns} FROM numbers({{rows}})",
    )


async def _run_schema(client: aiochlite.AsyncChClient, schema: Schema, rows: int) -> tuple[Result, int, int] | None:
    """Fetch one schema's payload and time both decoders over it.

    Returns:
        None when the schema has no run long enough to fuse, otherwise the timings, the payload
        size in bytes and the decoded row count.
    """
    payload = await client.fetch_format(schema.query.format(rows=rows), "RowBinaryWithNamesAndTypes")
    types, body = _read_header(payload)
    if tuple(types) != schema.types:
        raise RuntimeError(f"Server returned {types}, expected {list(schema.types)}")

    fused = _fused_decoder(types)
    if fused is None:
        return None

    per_field = _per_field_decoder(types)
    decoded = _check_agreement(per_field, fused, payload, body)
    return _measure(per_field, fused, payload, body, decoded), len(payload), decoded


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

        print(f"\n=== Representative schemas ({BENCH_ROWS:,} rows) ===")
        for schema in SCHEMAS:
            measured = await _run_schema(client, schema, BENCH_ROWS)
            if measured is None:
                print(f"\n{schema.label}\n  No run long enough to fuse; skipped.")
                continue
            result, payload_size, rows = measured
            _print_case(schema, payload_size, rows, result)

        print(f"\n=== Fusion gain by run length ({BENCH_SWEEP_ROWS:,} rows, UInt64 columns only) ===")
        print(f"\n  {'columns':>7}  {'per-field':>11}  {'fused':>11}  {'ratio':>6}")
        for width in range(2, 11):
            measured = await _run_schema(client, _sweep_schema(width), BENCH_SWEEP_ROWS)
            if measured is None:
                print(f"  {width:>7}  {'not fused':>11}")
                continue
            result, _, _ = measured
            per_median = statistics.median(result.per_field) * 1000
            fused_median = statistics.median(result.fused) * 1000
            print(f"  {width:>7}  {per_median:>8.2f} ms  {fused_median:>8.2f} ms  {per_median / fused_median:>5.2f}x")
    finally:
        await client.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
