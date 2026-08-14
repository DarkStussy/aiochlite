# aiochlite

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![GitHub License](https://img.shields.io/github/license/darkstussy/aiochlite?color=brightgreen)
[![PyPI - Version](https://img.shields.io/pypi/v/aiochlite?color=brightgreen)](https://pypi.org/project/aiochlite/)
[![PyPI - Downloads](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fshieldcn.dev%2Fpypi%2Fdm%2Faiochlite.json&query=%24.value&label=downloads&color=brightgreen&style=flat)](https://pypistats.org/packages/aiochlite)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/darkstussy/aiochlite/tests.yml?style=flat&label=Tests)
[![codecov](https://codecov.io/gh/darkstussy/aiochlite/branch/main/graph/badge.svg)](https://codecov.io/gh/darkstussy/aiochlite)
![GitHub last commit](https://img.shields.io/github/last-commit/darkstussy/aiochlite?color=brightgreen)

### Lightweight asynchronous ClickHouse client for Python built on aiohttp.

## Table of Contents

- [Features](#features)
- [Why aiochlite?](#why-aiochlite)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Basic Connection](#basic-connection)
  - [Execute Query](#execute-query)
  - [Insert Data](#insert-data)
  - [Fetch Results](#fetch-results)
  - [Export Formats](#export-formats)
  - [Query Parameters](#query-parameters)
  - [Query Settings](#query-settings)
  - [External Tables](#external-tables)
  - [JSON Type](#json-type)
  - [Error Handling](#error-handling)
  - [Custom Session](#custom-session)
  - [Enable Compression](#enable-compression)
- [Type Conversion](#type-conversion)
- [Benchmarks](#benchmarks)
- [License](#license)

## Features

- **Lightweight** - minimal dependencies, only aiohttp required
- **Streaming support** - efficient processing of large datasets with `.stream()`
- **Export formats** - raw Parquet / CSV / TSV / JSON / Arrow / ORC payloads via `.fetch_format()` and `.stream_format()`
- **External tables** - advanced temporary data support
- **Type conversion** - automatic conversion between Python and ClickHouse types
- **Type-safe** - full type hints coverage
- **Flexible** - custom sessions, compression, query settings

## Why aiochlite?

A small, pure-Python async client for ClickHouse over HTTP. Results are decoded from
`RowBinaryWithNamesAndTypes` into either `Row` wrappers (`fetch()`) or raw tuples (`fetch_rows()`).

- **One dependency**: `aiohttp`. aiochlite itself ships no compiled extensions.
- **Server-side query parameters**: values are sent as ClickHouse `param_*` and never interpolated into the query text.
- **Fast for pure Python**: in the [benchmark below](#benchmarks), `fetch_rows()` spends 24% less time than `aiochclient` and `fetch()` 16% less, against a client with C-accelerated parsing.
- **Typed**: complete type hints for IDEs and static type checkers.
- **Focused API**: ClickHouse over HTTP, without pandas, numpy, Arrow or Polars integrations.

**Choosing a client.** For maximum throughput or DataFrame integrations, use the official
[clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect): it also has a real asyncio
client, and in the same benchmark its compiled parser is 1.7x faster than `fetch_rows()` and 1.9x
faster than `fetch()`. Reach for aiochlite when you want a small async client with a single
dependency that just returns rows.

## Installation

```bash
pip install aiochlite
```

Optionally, pull in `aiohttp`'s own `speedups` extra (`aiodns`, `Brotli`, and `zstd` support on
Python < 3.14):

```bash
pip install "aiochlite[aiohttp-speedups]"
```

It affects connection setup and, with `enable_compression=True`, the available response encodings.
Row decoding is pure Python either way and runs at the same speed.

## Quick Start

### Basic Connection

```python
from aiochlite import AsyncChClient

# Using context manager (recommended)
async with AsyncChClient(
    url="http://localhost:8123",
    user="default",
    password="",
    database="default"
) as client:
    result = await client.fetch("SELECT 1")

# Or manual connection management
client = AsyncChClient("http://localhost:8123")
try:
    assert await client.ping()
    result = await client.fetch("SELECT 1")
finally:
    await client.close()
```

### Execute Query

```python
await client.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id UInt32,
        name String,
        email String
    ) ENGINE = MergeTree() ORDER BY id
""")
```

### Insert Data

```python
# Insert dictionaries
data = [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"},
]
await client.insert("users", data)

# Insert tuples
data = [
    (3, "Charlie", "charlie@example.com"),
    (4, "Diana", "diana@example.com"),
]
await client.insert("users", data, column_names=["id", "name", "email"])

# Insert with settings
await client.insert(
    "users",
    [{"id": 5, "name": "Eve", "email": "eve@example.com"}],
    settings={"max_insert_block_size": 100000}
)
```

### Fetch Results

```python
# Fetch all rows
rows = await client.fetch("SELECT * FROM users")
for row in rows:
    print(f"ID: {row.id}, Name: {row.name}, Email: {row.email}")

# Fetch one row
row = await client.fetchone("SELECT * FROM users WHERE id = 1")
if row:
    print(row.name)  # Attribute access
    print(row["name"])  # Dictionary-style access
    print(row.first())  # Get first column value

# Fetch single value
count = await client.fetchval("SELECT count() FROM users")
print(f"Total users: {count}")

# Iterate over results (for large datasets)
async for row in client.stream("SELECT * FROM users"):
    print(row.name)
```

Rows are decoded in full as they arrive. If a query selects many more columns than you actually
read — a wide `SELECT *` where only a few fields are used — `lazy_decode=True` decodes each cell
on first access instead:

```python
client = AsyncChClient("http://localhost:8123", lazy_decode=True)
```

In the benchmark shapes it started paying off below roughly a third of the selected columns and
cost up to 45% when every column was read, but the break-even point depends on the column types
and on how expensive the skipped ones are to decode. Leave it off unless your access pattern
clearly matches, and measure your own query.

### Export Formats

Get the raw server payload in any ClickHouse output format — Parquet, CSV, TSV, JSON, Arrow, ORC and more.
Both methods return raw `bytes` exactly as produced by the server (decode text formats yourself).

```python
# Whole result at once
parquet = await client.fetch_format("SELECT * FROM users", "Parquet")
csv = (await client.fetch_format("SELECT * FROM users", "CSVWithNames")).decode()

# Chunked streaming for large result sets
with open("users.parquet", "wb") as f:
    async for chunk in client.stream_format("SELECT * FROM users", "Parquet"):
        f.write(chunk)

# Query parameters, settings and external tables work as usual
ndjson = await client.fetch_format(
    "SELECT * FROM users WHERE id > {id:UInt32}",
    "JSONEachRow",
    params={"id": 10},
)
```

**Supported formats** (`ExportFormat`):

| Group | Formats |
|-------|---------|
| Columnar / binary | `Parquet`, `Arrow`, `ArrowStream`, `ORC`, `Avro`, `Native`, `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes` |
| Separated values | `CSV`, `CSVWithNames`, `CSVWithNamesAndTypes`, `TSV`, `TSVWithNames`, `TSVWithNamesAndTypes`, `TabSeparated`, `TabSeparatedWithNames`, `TabSeparatedWithNamesAndTypes`, `TSKV`, `Values` |
| JSON | `JSON`, `JSONStrings`, `JSONCompact`, `JSONColumns`, `JSONEachRow`, `JSONStringsEachRow`, `JSONObjectEachRow`, `JSONCompactEachRow`, `JSONCompactEachRowWithNames`, `JSONCompactEachRowWithNamesAndTypes` |
| Human-readable | `XML`, `Markdown`, `Vertical`, `Pretty`, `PrettyCompact` |

Any other output format the server accepts can still be passed at runtime (type checkers will flag it).

> [!WARNING]
> `fetch_parquet()` and `stream_parquet()` are deprecated and will be removed in a future release.
> Use `fetch_format(query, "Parquet")` and `stream_format(query, "Parquet")` instead.

### Query Parameters

```python
# Basic types
result = await client.fetch(
    "SELECT * FROM users WHERE id = {id:UInt32}",
    params={"id": 1}
)

# Lists and tuples (arrays)
result = await client.fetch(
    "SELECT * FROM users WHERE id IN {ids:Array(UInt32)}",
    params={"ids": [1, 2, 3]}  # or tuple: (1, 2, 3)
)

# Datetime and date
from datetime import datetime, date

result = await client.fetch(
    "SELECT * FROM events WHERE created_at > {dt:DateTime} AND date = {d:Date}",
    params={
        "dt": datetime(2025, 12, 14, 15, 30, 45),
        "d": date(2025, 12, 14)
    }
)

# UUID
from uuid import UUID

result = await client.fetch(
    "SELECT * FROM users WHERE uuid = {uid:UUID}",
    params={"uid": UUID("550e8400-e29b-41d4-a716-446655440000")}
)

# Decimal
from decimal import Decimal

result = await client.fetch(
    "SELECT * FROM products WHERE price > {price:Decimal(10, 2)}",
    params={"price": Decimal("99.99")}
)

# Nested arrays and maps
result = await client.fetch(
    "SELECT {matrix:Array(Array(Int32))} AS matrix, {data:Map(String, Int32)} AS data",
    params={
        "matrix": [[1, 2], [3, 4]],
        "data": {"a": 1, "b": 2}
    }
)
```

**Supported parameter types:**
- Basic: `int`, `float`, `str`, `bool`, `None`
- Collections: `list`, `tuple`, `dict`
- Date/Time: `datetime`, `date`, `timedelta`
- Special: `UUID`, `Decimal`, `bytes`

See [Type Conversion](#type-conversion) for full type mapping details.

### Query Settings

```python
rows = await client.fetch(
    "SELECT * FROM users",
    settings={
        "max_execution_time": 60,
        "max_block_size": 10000
    }
)
```

### External Tables

```python
from aiochlite import ExternalTable

external_data = {
    "temp_data": ExternalTable(
        structure=[("id", "UInt32"), ("value", "String")],
        data=[
            {"id": 1, "value": "foo"},
            {"id": 2, "value": "bar"},
        ]
    )
}

result = await client.fetch(
    """
    SELECT t1.id, t1.name, t2.value
    FROM users t1
    JOIN temp_data t2 ON t1.id = t2.id
    """,
    external_tables=external_data
)
```

### JSON Type

> [!NOTE]
> For ClickHouse versions where `JSON` is still considered experimental, set `allow_experimental_json_type=1` via client settings.

```python
await client.execute("DROP TABLE IF EXISTS json_demo")
await client.execute("CREATE TABLE json_demo (id UInt32, doc JSON) ENGINE = Memory")

await client.insert(
    "json_demo",
    [{"id": 1, "doc": {"a": 1, "b": [True, None, {"c": "x"}]}}],
)

row = await client.fetchone("SELECT id, doc FROM json_demo WHERE id = 1")
print(row["doc"])  # Output: {"a": 1, "b": [True, None, {"c": "x"}]}
```

### Error Handling

```python
from aiochlite import ChClientError

try:
    await client.execute("SELECT * FROM non_existent_table")
except ChClientError as e:
    print(f"Query failed: {e}")
```

### Custom Session

```python
from aiohttp import ClientSession, ClientTimeout

timeout = ClientTimeout(total=30)
async with ClientSession(timeout=timeout) as session:
    async with AsyncChClient(url="http://localhost:8123", session=session) as client:
        result = await client.fetch("SELECT 1")
```

### Enable Compression

```python
async with AsyncChClient(url="http://localhost:8123", enable_compression=True) as client:
    result = await client.fetch("SELECT * FROM users")
```

## Type Conversion

aiochlite uses ClickHouse’s `RowBinaryWithNamesAndTypes` for result decoding:

- `fetch`, `fetchone`, `fetchval`, `stream` automatically append `FORMAT RowBinaryWithNamesAndTypes` and decode rows into Python values.
- Queries passed to these methods must not contain a `FORMAT ...` clause.
- Use `execute()` for statements that don’t return rows.

**Automatic type conversion from ClickHouse:**

| ClickHouse Type | Python Type | Notes |
|----------------|-------------|-------|
| **Numeric** | | |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | `int` | |
| `Int8`, `Int16`, `Int32`, `Int64` | `int` | |
| `Float32`, `Float64` | `float` | |
| `Decimal(P, S)` | `Decimal` | Precision preserved |
| `Decimal32(S)`, `Decimal64(S)`, `Decimal128(S)`, `Decimal256(S)` | `Decimal` | Precision preserved |
| **String** | | |
| `String` | `str` | |
| `FixedString(N)` | `str` | Null padding stripped |
| **Date/Time** | | |
| `Date` | `date` | |
| `Date32` | `date` | |
| `DateTime` | `datetime` | `tzinfo` only if the type includes a timezone |
| `DateTime64(P)` | `datetime` | `tzinfo` only if the type includes a timezone |
| `Time` | `timedelta` | Signed seconds; supports values beyond 24h |
| `Time64(P)` | `timedelta` | `timedelta` is microsecond-precision, so `P > 6` is truncated |
| **Special** | | |
| `UUID` | `UUID` | |
| `IPv4` | `ipaddress.IPv4Address` | |
| `IPv6` | `ipaddress.IPv6Address` | |
| `Enum8`, `Enum16` | `str` | Enum value name |
| `Bool` | `bool` | |
| **Composite** | | |
| `Array(T)` | `list` | Elements converted recursively |
| `Tuple(T1, T2, ...)` | `tuple` | Elements converted recursively |
| `Map(K, V)` | `dict` | Keys and values converted |
| **Modifiers** | | |
| `Nullable(T)` | `T \| None` | Nulls become `None` |
| `LowCardinality(T)` | `T` | Transparent wrapper |
| **Other** | | |
| `JSON` | `Any` | `json.loads()` result |

**Python to ClickHouse conversion:**

When sending data to ClickHouse (query parameters and inserts), Python types are automatically converted:

- `datetime` → `YYYY-MM-DD HH:MM:SS`
- `date` → `YYYY-MM-DD`
- `timedelta` → `HH:MM:SS[.ffffff]` (signed; suitable for `Time` / `Time64`)
- `UUID` / `Decimal` → string representation
- `list` → array literal (e.g. `[1,2,3]`)
- `tuple` → tuple literal (e.g. `(1,2,3)`)
- `dict` → map literal (e.g. `{'k':'v'}`)
- `bytes` → UTF-8 decoded string
- `None` → `NULL`
- `bool` → `1`/`0` for query parameters, `true`/`false` inside container literals

## Benchmarks

Benchmark scripts live in [benchmarks/](benchmarks/).

> [!NOTE]
> Benchmarks always depend on machine and environment (CPU, RAM, kernel, ClickHouse version/config, network, etc).
> These results were captured on a local machine with 8 CPU cores (16 threads) and 32 GB RAM, running
> ClickHouse 26.3 LTS. Each client uses the configuration recommended by its documentation, including
> the `aiohttp-speedups` extra for `aiochclient`.

Latest fetch-and-decode results for 100,000 rows (5 rounds, measured 2026-08-14):

| Client | Average | Throughput | Time per row |
| --- | ---: | ---: | ---: |
| `clickhouse-connect` (async) | 161.19 ms | 620,388 rows/s | 1.6 µs |
| `aiochlite` (tuples) | 275.79 ms | 362,598 rows/s | 2.8 µs |
| `aiochlite` (`Row`) | 303.41 ms | 329,584 rows/s | 3.0 µs |
| `aiochclient` | 363.21 ms | 275,324 rows/s | 3.6 µs |

Versions: `aiochlite` 1.4.0, `clickhouse-connect` 1.7.1, `aiochclient` 2.7.0, Python 3.14.5,
and ClickHouse 26.3.17.110.

`clickhouse-connect` includes compiled C extensions. In contrast, `aiochlite` is pure Python and has a single
dependency: `aiohttp`.

## License

MIT License

Copyright (c) 2025 darkstussy
