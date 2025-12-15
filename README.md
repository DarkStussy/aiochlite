# aiochlite

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![GitHub License](https://img.shields.io/github/license/darkstussy/aiochlite?color=brightgreen)
[![PyPI - Version](https://img.shields.io/pypi/v/aiochlite?color=brightgreen)](https://pypi.org/project/aiochlite/)
![PyPI - Downloads](https://img.shields.io/pypi/dm/aiochlite?style=flat&color=brightgreen)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/darkstussy/aiochlite/tests.yml?style=flat&label=Tests)
![GitHub last commit](https://img.shields.io/github/last-commit/darkstussy/aiochlite?color=brightgreen)

### Lightweight asynchronous ClickHouse client for Python built on aiohttp.

> **Beta notice:** APIs and behavior may change; expect sharp edges while things settle.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Basic Connection](#basic-connection)
  - [Execute Query](#execute-query)
  - [Insert Data](#insert-data)
  - [Fetch Results](#fetch-results)
  - [Query Parameters](#query-parameters)
  - [Query Settings](#query-settings)
  - [External Tables](#external-tables)
  - [Error Handling](#error-handling)
  - [Custom Session](#custom-session)
  - [Enable Compression](#enable-compression)
- [Type Conversion](#type-conversion)
- [License](#license)

## Features

- **Lightweight** - minimal dependencies, only aiohttp required
- **Streaming support** - efficient processing of large datasets with `.stream()`
- **External tables** - advanced temporary data support
- **Type conversion** - automatic conversion between Python and ClickHouse types
- **Type-safe** - full type hints coverage
- **Flexible** - custom sessions, compression, query settings

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
- `bool` → `1` (True) or `0` (False)

## License

MIT License

Copyright (c) 2025 darkstussy
