# aiochlite

Lightweight asynchronous ClickHouse client for Python built on aiohttp.

## Features

- **Lightweight** - minimal dependencies, only aiohttp required
- **Streaming support** - efficient processing of large datasets with `.stream()`
- **External tables** - advanced temporary data support
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
result = await client.fetch(
    "SELECT * FROM users WHERE id = {id:UInt32}",
    params={"id": 1}
)
```

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
session = ClientSession(timeout=timeout)

async with AsyncChClient(url="http://localhost:8123", session=session) as client:
    result = await client.fetch("SELECT 1")
```

### Enable Compression

```python
async with AsyncChClient(url="http://localhost:8123", enable_compression=True) as client:
    result = await client.fetch("SELECT * FROM users")
```

## License

MIT License

Copyright (c) 2025 darkstussy