# Benchmarks

This directory contains benchmark scripts for `aiochlite`:

- `fetch_rows.py` — end-to-end fetch + decode, compared against other clients.
- `converter_cache.py` — decoding only, measuring both sides of the value-cache trade.

> [!NOTE]
> Benchmarks always depend on machine and environment (CPU, RAM, kernel, ClickHouse version/config, network, etc).
> The sample output was captured on a local machine with 8 CPU cores (16 threads) and 32 GB RAM. ClickHouse
> 26.3 LTS ran on the same host. Use the results to compare clients within this run; absolute timings will vary
> on other systems.

## Methodology

- Each client uses the configuration recommended by its documentation. In particular, install `aiochclient` with
  the `aiohttp-speedups` extra. Without it, date parsing uses a pure-Python fallback and the benchmark is roughly
  four times slower, resulting in a misleading comparison.
- Competitor packages are intentionally excluded from `pyproject.toml`. Install them separately:

  ```bash
  uv pip install "aiochclient[aiohttp-speedups]" clickhouse-connect
  ```

  Run the script directly with `.venv/bin/python`. Using `uv run` re-syncs the environment from `uv.lock` and
  removes packages that are not declared there.
- The output header records the client, Python, and ClickHouse versions, making published results reproducible.
- When refreshing the results, update the measurement date and version line in this file, then update the root
  [README.md](../README.md) as well.

## IO benchmark: fetch + decode

Script: `benchmarks/fetch_rows.py`

What it measures:
- End-to-end `SELECT` fetch + decode time.
- Data is generated on the server (`INSERT ... SELECT ... FROM numbers(...)`) to avoid measuring client-side inserts.
- Compares:
  - `aiochlite (Row)`: `AsyncChClient.fetch()` (returns `Row`), in the client's default decode mode.
  - `aiochlite (tuples)`: `AsyncChClient.fetch_rows()` (returns raw tuples)
  - `clickhouse-connect (async)`
  - `aiochclient`

Run:

```bash
.venv/bin/python benchmarks/fetch_rows.py
```

Tune (optional):

```bash
BENCH_ROWS=1000000 BENCH_ROUNDS=10 BENCH_WARMUP=3 .venv/bin/python benchmarks/fetch_rows.py
```

Environment variables:
- `CLICKHOUSE_HOST` (default: `localhost`)
- `CLICKHOUSE_PORT` (default: `8123`)
- `CLICKHOUSE_USER` (default: `default`)
- `CLICKHOUSE_PASSWORD` (default: empty)
- `CLICKHOUSE_DATABASE` (default: `default`)
- `BENCH_ROWS` (default: `100000`)
- `BENCH_ROUNDS` (default: `5`)
- `BENCH_WARMUP` (default: `2`)

### Sample output

Measured 2026-08-17.

```
Clients: aiochlite 1.7.0, aiochclient 2.7.0, clickhouse-connect 1.7.1
Python: 3.14.5, ClickHouse: 26.3.17.110
Rows: 100000, rounds: 5, warmup: 2
Table: bench_io_161df20e45144d1caaf710f018b27da4

IO benchmark (clickhouse-connect (async))
Round 1:   171.93 ms (581,638 rows/s, 1.7 µs/row)
Round 2:   176.51 ms (566,536 rows/s, 1.8 µs/row)
Round 3:   160.21 ms (624,181 rows/s, 1.6 µs/row)
Round 4:   158.37 ms (631,420 rows/s, 1.6 µs/row)
Round 5:   158.09 ms (632,552 rows/s, 1.6 µs/row)
Avg:       165.02 ms (605,978 rows/s, 1.7 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   219.15 ms (456,310 rows/s, 2.2 µs/row)
Round 2:   200.60 ms (498,498 rows/s, 2.0 µs/row)
Round 3:   196.46 ms (509,008 rows/s, 2.0 µs/row)
Round 4:   223.52 ms (447,379 rows/s, 2.2 µs/row)
Round 5:   198.13 ms (504,724 rows/s, 2.0 µs/row)
Avg:       207.57 ms (481,759 rows/s, 2.1 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   166.34 ms (601,187 rows/s, 1.7 µs/row)
Round 2:   165.57 ms (603,984 rows/s, 1.7 µs/row)
Round 3:   164.75 ms (606,973 rows/s, 1.6 µs/row)
Round 4:   164.96 ms (606,211 rows/s, 1.6 µs/row)
Round 5:   171.40 ms (583,425 rows/s, 1.7 µs/row)
Avg:       166.60 ms (600,228 rows/s, 1.7 µs/row)

IO benchmark (aiochclient)
Round 1:   363.21 ms (275,322 rows/s, 3.6 µs/row)
Round 2:   369.87 ms (270,365 rows/s, 3.7 µs/row)
Round 3:   369.62 ms (270,547 rows/s, 3.7 µs/row)
Round 4:   362.14 ms (276,139 rows/s, 3.6 µs/row)
Round 5:   363.02 ms (275,466 rows/s, 3.6 µs/row)
Avg:       365.57 ms (273,544 rows/s, 3.7 µs/row)
```

Repeat runs of the same configuration produced averages within approximately 2% of these results.
`aiochlite (tuples)` and `clickhouse-connect` land inside that band of each other, so read them as a tie
rather than one leading the other. The gap to `aiochlite (Row)` is the `Row` wrapper, one object per row.

## Converter benchmark: the value cache

Script: `benchmarks/converter_cache.py`

What it measures:
- Both decoders come from `_row_decoder`, the compiled path a query takes for this schema, so the conversion is
  measured against the decode that really surrounds it.
- Fixed-width columns such as `DateTime` and `Decimal` arrive as integers and are turned into Python objects by a
  converter. Converters built per query memoize through `_value_cache`, which holds up to
  `_QUERY_VALUE_CACHE_SIZE` values per column and is released with the query. Converters reached through
  `_reader_for_type` outlive the query that built them and keep their own bound (`_VALUE_CACHE_SIZE`).
- The same schema is decoded twice, by a row reader whose converters memoize and by one whose converters do not.
  The uncached variant is built by neutralizing `_value_cache` while the converters are created, so both readers
  run identical conversion logic; the script asserts the patch took effect and that both decoders agree.
- Two payloads: one where values repeat heavily, one where every value is distinct.
- Timing method: agreement check outside the timer, alternating which decoder runs first, previous result
  released before the next round, medians next to the raw series.

Run:

```bash
.venv/bin/python benchmarks/converter_cache.py
```

### Sample output

Measured 2026-08-17.

```
aiochlite 1.7.0 @ ef5793a
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Schema: UInt64, DateTime('UTC'), Decimal(18, 2)
Rows: 200000, rounds: 7, warmup: 2
ClickHouse: 26.3.17.110

Low cardinality — 200 distinct timestamps, 100 distinct prices
  Cached:   median   23.55 ms  [24.17, 23.55, 23.23, 25.09, 23.72, 23.53, 23.21]
  Uncached: median  117.86 ms  [118.97, 117.84, 119.22, 117.86, 118.21, 116.49, 115.26]
  Cache changes decode time by -80.0%

High cardinality — every timestamp and price distinct
  Cached:   median  166.44 ms  [166.17, 165.96, 168.29, 166.44, 163.86, 172.39, 170.74]
  Uncached: median  115.72 ms  [115.72, 115.10, 114.84, 114.97, 120.08, 119.10, 119.82]
  Cache changes decode time by +43.8%
```

The two sides of the trade are both large here: repeated values make the cache save 80%, all-distinct values
make it cost 44%. Two of the three columns go through a converter, so conversion dominates the decode; on a wide
schema where one column in ten is a `DateTime` both numbers shrink. Measure it rather than assume it.

The decoders are rebuilt every round on purpose. Sharing one across rounds leaves its cache warm from the round
before, and the high-cardinality case then reports hits where a single query has only misses — it read as a 70%
saving instead of a cost.

Neither payload covers what happens between them, which is what decided the cache's shape. Three ways to
bound it, on 300k rows of the schema above, against 174 ms with no cache at all:

| Distinct values | Order | `lru_cache` | Fill and stop | Fill and start over |
| --- | --- | ---: | ---: | ---: |
| 20,000 | random | 224 ms | 52 ms | 51 ms |
| 100,000 | random | — | 152 ms | 252 ms |
| 100,000 | sorted | — | 150 ms | 110 ms |
| 100,000 then 100 new hot ones | — | — | 235 ms | 108 ms |

Eviction collapses: at 20k distinct a 4096-entry `lru_cache` is slower than no cache, every lookup missing and
every insert evicting. Filling and stopping avoids that but keeps whatever it saw first, so it misses forever
once the working set moves — the last two rows, which are what a column in time order looks like. Starting over
loses only where access is uniformly random over a cardinality just above the bound. `_QUERY_VALUE_CACHE_SIZE`
takes the last of the three. A payload for each row above is worth adding.

The bound is also what keeps `stream()` flat, where the cache lives as long as the query while the caller drops
each row. Peak traced memory:

| Rows | Unbounded | Bounded |
| --- | ---: | ---: |
| 100,000 | 36.6 MiB | 23.4 MiB |
| 1,000,000 | 297.4 MiB | 23.6 MiB |
| 3,000,000 | 1033.3 MiB | 24.4 MiB |
