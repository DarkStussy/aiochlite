# aiochlite

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![GitHub License](https://img.shields.io/github/license/darkstussy/aiochlite?color=brightgreen)
[![PyPI - Version](https://img.shields.io/pypi/v/aiochlite?color=brightgreen)](https://pypi.org/project/aiochlite/)
![PyPI - Downloads](https://img.shields.io/pypi/dm/aiochlite?style=flat&color=brightgreen)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/darkstussy/aiochlite/tests.yml?style=flat&label=Tests)
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
- **External tables** - advanced temporary data support
- **Type conversion** - automatic conversion between Python and ClickHouse types
- **Type-safe** - full type hints coverage
- **Flexible** - custom sessions, compression, query settings

## Why aiochlite?

- **Real asyncio I/O**: built on `aiohttp` without wrapping blocking code in a thread pool.
- **Fast decoding**: uses `RowBinaryWithNamesAndTypes` and lets you choose between `Row` wrappers (`fetch()`) and raw tuples (`fetch_rows()`).
- **Small surface area**: minimal dependencies and a focused API for ClickHouse HTTP.

Notes on alternatives (at the time of writing):
- `clickhouse-connect` async client runs the synchronous driver in a `ThreadPoolExecutor` (good ergonomics, but not truly non-blocking asyncio I/O).
- `aiochclient` appears unmaintained and is significantly slower in our IO benchmarks (see [Benchmarks](#benchmarks)).
- Other clients/libraries often trade off either true non-blocking asyncio I/O or raw performance (e.g. sync-only APIs, JSON/text formats, or extra abstraction overhead).

## Installation

```bash
pip install aiochlite
```

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
- Date/Time: `datetime`, `date`
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
> The sample output was captured on a local machine with 6 CPU cores and 32 GB RAM, running ClickHouse 25.8 LTS.

Latest results:

- `clickhouse-connect (async)`: Avg: `433.35 ms (230,761 rows/s, 4.3 µs/row)`
- `aiochlite (Row)`: Avg: `521.28 ms (191,834 rows/s, 5.2 µs/row)`
- `aiochlite (tuples)`: Avg: `461.25 ms (216,801 rows/s, 4.6 µs/row)`
- `aiochclient`: Avg: `1558.77 ms (64,153 rows/s, 15.6 µs/row)`

## License

MIT License

Copyright (c) 2025 darkstussy
