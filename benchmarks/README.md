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
Clients: aiochlite 1.6.0, aiochclient 2.7.0, clickhouse-connect 1.7.1
Python: 3.14.5, ClickHouse: 26.3.17.110
Rows: 100000, rounds: 5, warmup: 2
Table: bench_io_8a1af16915e54010ba65fe7fdc4e013c

IO benchmark (clickhouse-connect (async))
Round 1:   171.97 ms (581,500 rows/s, 1.7 µs/row)
Round 2:   162.36 ms (615,921 rows/s, 1.6 µs/row)
Round 3:   169.83 ms (588,810 rows/s, 1.7 µs/row)
Round 4:   159.66 ms (626,341 rows/s, 1.6 µs/row)
Round 5:   156.65 ms (638,370 rows/s, 1.6 µs/row)
Avg:       164.09 ms (609,408 rows/s, 1.6 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   196.90 ms (507,865 rows/s, 2.0 µs/row)
Round 2:   208.32 ms (480,035 rows/s, 2.1 µs/row)
Round 3:   201.75 ms (495,652 rows/s, 2.0 µs/row)
Round 4:   195.17 ms (512,380 rows/s, 2.0 µs/row)
Round 5:   208.43 ms (479,781 rows/s, 2.1 µs/row)
Avg:       202.11 ms (494,770 rows/s, 2.0 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   164.17 ms (609,141 rows/s, 1.6 µs/row)
Round 2:   164.83 ms (606,671 rows/s, 1.6 µs/row)
Round 3:   165.00 ms (606,076 rows/s, 1.6 µs/row)
Round 4:   165.92 ms (602,683 rows/s, 1.7 µs/row)
Round 5:   167.21 ms (598,055 rows/s, 1.7 µs/row)
Avg:       165.43 ms (604,501 rows/s, 1.7 µs/row)

IO benchmark (aiochclient)
Round 1:   365.63 ms (273,504 rows/s, 3.7 µs/row)
Round 2:   365.80 ms (273,373 rows/s, 3.7 µs/row)
Round 3:   366.52 ms (272,833 rows/s, 3.7 µs/row)
Round 4:   368.75 ms (271,188 rows/s, 3.7 µs/row)
Round 5:   368.66 ms (271,256 rows/s, 3.7 µs/row)
Avg:       367.07 ms (272,427 rows/s, 3.7 µs/row)
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
  converter. Converters built per query memoize through `_value_cache`, which holds every distinct value the query
  produced and is released with it. Only converters reached through `_reader_for_type`, which outlive the query
  that built them, keep a bound (`_VALUE_CACHE_SIZE`).
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
aiochlite 1.6.0 @ 0dc16e1
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Schema: UInt64, DateTime('UTC'), Decimal(18, 2)
Rows: 200000, rounds: 7, warmup: 2
ClickHouse: 26.3.17.110

Low cardinality — 200 distinct timestamps, 100 distinct prices
  Cached:   median   22.91 ms  [23.11, 22.91, 22.61, 22.92, 22.52, 23.30, 22.61]
  Uncached: median  117.26 ms  [116.79, 117.26, 118.23, 124.67, 115.48, 115.27, 120.18]
  Cache changes decode time by -80.5%

High cardinality — every timestamp and price distinct
  Cached:   median  176.66 ms  [185.20, 190.57, 173.56, 184.32, 172.30, 176.66, 170.87]
  Uncached: median  127.68 ms  [143.86, 127.68, 122.65, 134.90, 139.99, 123.79, 125.26]
  Cache changes decode time by +38.4%
```

The two sides of the trade are both large here: repeated values make the cache save 80%, all-distinct values
make it cost 41%. Two of the three columns go through a converter, so conversion dominates the decode; on a wide
schema where one column in ten is a `DateTime` both numbers shrink. Measure it rather than assume it.

The decoders are rebuilt every round on purpose. Sharing one across rounds leaves its cache warm from the round
before, and the high-cardinality case then reports hits where a single query has only misses — it read as a 70%
saving instead of a 41% cost.

Neither payload covers the case that decided the cache's shape: a cardinality just above the old bound, where
every lookup misses and every insert evicts. At 20k distinct, 200k `DateTime` values took 51.7 ms through a
4096-entry `lru_cache`, against 39.5 ms with no cache and 8.8 ms with no bound. A third payload there is worth
adding.
