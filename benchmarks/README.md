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
- Three schemas, run separately, because decode cost depends far more on column shape than on column count:
  - `flat columns`: `UInt64, DateTime('UTC'), Tuple(String, UInt16), Array(Decimal(10, 2))`
  - `wide strings`: `UInt64` and nine `String` columns
  - `nested containers`: `UInt64, Array(Array(UInt8)), Map(String, Array(UInt8)), Array(Nullable(UInt64))`
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

=== Schema: flat columns — id UInt64, event_time DateTime('UTC'), payload Tuple(String, UInt16), prices Array(Decimal(10, 2))
Table: bench_io_770aea740b8b4eabba2910fcb0cb6c13

IO benchmark (clickhouse-connect (async))
Round 1:   149.43 ms (669,222 rows/s, 1.5 µs/row)
Round 2:   148.28 ms (674,407 rows/s, 1.5 µs/row)
Round 3:   160.86 ms (621,641 rows/s, 1.6 µs/row)
Round 4:   148.83 ms (671,886 rows/s, 1.5 µs/row)
Round 5:   145.30 ms (688,232 rows/s, 1.5 µs/row)
Avg:       150.54 ms (664,271 rows/s, 1.5 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   181.12 ms (552,107 rows/s, 1.8 µs/row)
Round 2:   179.14 ms (558,208 rows/s, 1.8 µs/row)
Round 3:   181.17 ms (551,956 rows/s, 1.8 µs/row)
Round 4:   179.89 ms (555,905 rows/s, 1.8 µs/row)
Round 5:   183.22 ms (545,803 rows/s, 1.8 µs/row)
Avg:       180.91 ms (552,763 rows/s, 1.8 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   153.18 ms (652,845 rows/s, 1.5 µs/row)
Round 2:   161.48 ms (619,259 rows/s, 1.6 µs/row)
Round 3:   150.59 ms (664,038 rows/s, 1.5 µs/row)
Round 4:   149.21 ms (670,208 rows/s, 1.5 µs/row)
Round 5:   154.23 ms (648,379 rows/s, 1.5 µs/row)
Avg:       153.74 ms (650,456 rows/s, 1.5 µs/row)

IO benchmark (aiochclient)
Round 1:   333.15 ms (300,168 rows/s, 3.3 µs/row)
Round 2:   342.36 ms (292,092 rows/s, 3.4 µs/row)
Round 3:   333.23 ms (300,091 rows/s, 3.3 µs/row)
Round 4:   331.88 ms (301,313 rows/s, 3.3 µs/row)
Round 5:   330.57 ms (302,503 rows/s, 3.3 µs/row)
Avg:       334.24 ms (299,187 rows/s, 3.3 µs/row)

=== Schema: wide strings — id UInt64, s0 String, s1 String, s2 String, s3 String, s4 String, s5 String, s6 String, s7 String, s8 String
Table: bench_io_dbe5fe8dc4644afe81668e197bdd053e

IO benchmark (clickhouse-connect (async))
Round 1:    77.90 ms (1,283,697 rows/s, 0.8 µs/row)
Round 2:    78.51 ms (1,273,663 rows/s, 0.8 µs/row)
Round 3:    71.93 ms (1,390,181 rows/s, 0.7 µs/row)
Round 4:    70.95 ms (1,409,375 rows/s, 0.7 µs/row)
Round 5:    70.61 ms (1,416,272 rows/s, 0.7 µs/row)
Avg:        73.98 ms (1,351,687 rows/s, 0.7 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   164.43 ms (608,155 rows/s, 1.6 µs/row)
Round 2:   168.60 ms (593,136 rows/s, 1.7 µs/row)
Round 3:   164.13 ms (609,290 rows/s, 1.6 µs/row)
Round 4:   166.40 ms (600,980 rows/s, 1.7 µs/row)
Round 5:   166.24 ms (601,558 rows/s, 1.7 µs/row)
Avg:       165.96 ms (602,567 rows/s, 1.7 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   116.16 ms (860,897 rows/s, 1.2 µs/row)
Round 2:   117.17 ms (853,471 rows/s, 1.2 µs/row)
Round 3:   114.63 ms (872,335 rows/s, 1.1 µs/row)
Round 4:   116.22 ms (860,447 rows/s, 1.2 µs/row)
Round 5:   117.89 ms (848,221 rows/s, 1.2 µs/row)
Avg:       116.41 ms (858,997 rows/s, 1.2 µs/row)

IO benchmark (aiochclient)
Round 1:   255.75 ms (391,012 rows/s, 2.6 µs/row)
Round 2:   257.46 ms (388,410 rows/s, 2.6 µs/row)
Round 3:   255.23 ms (391,797 rows/s, 2.6 µs/row)
Round 4:   257.64 ms (388,144 rows/s, 2.6 µs/row)
Round 5:   258.77 ms (386,445 rows/s, 2.6 µs/row)
Avg:       256.97 ms (389,152 rows/s, 2.6 µs/row)

=== Schema: nested containers — id UInt64, nested Array(Array(UInt8)), tags Map(String, Array(UInt8)), opt Array(Nullable(UInt64))
Table: bench_io_d343f660a1144ce6a23a918172f4126c

IO benchmark (clickhouse-connect (async))
Round 1:   121.24 ms (824,824 rows/s, 1.2 µs/row)
Round 2:   127.71 ms (783,034 rows/s, 1.3 µs/row)
Round 3:   121.48 ms (823,215 rows/s, 1.2 µs/row)
Round 4:   125.67 ms (795,733 rows/s, 1.3 µs/row)
Round 5:   124.02 ms (806,307 rows/s, 1.2 µs/row)
Avg:       124.02 ms (806,303 rows/s, 1.2 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   188.57 ms (530,297 rows/s, 1.9 µs/row)
Round 2:   205.34 ms (487,006 rows/s, 2.1 µs/row)
Round 3:   192.25 ms (520,156 rows/s, 1.9 µs/row)
Round 4:   193.38 ms (517,119 rows/s, 1.9 µs/row)
Round 5:   191.45 ms (522,320 rows/s, 1.9 µs/row)
Avg:       194.20 ms (514,937 rows/s, 1.9 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   161.47 ms (619,313 rows/s, 1.6 µs/row)
Round 2:   161.76 ms (618,193 rows/s, 1.6 µs/row)
Round 3:   161.55 ms (619,011 rows/s, 1.6 µs/row)
Round 4:   160.33 ms (623,713 rows/s, 1.6 µs/row)
Round 5:   166.24 ms (601,537 rows/s, 1.7 µs/row)
Avg:       162.27 ms (616,257 rows/s, 1.6 µs/row)

IO benchmark (aiochclient)
Round 1:   344.27 ms (290,469 rows/s, 3.4 µs/row)
Round 2:   345.12 ms (289,756 rows/s, 3.5 µs/row)
Round 3:   347.16 ms (288,052 rows/s, 3.5 µs/row)
Round 4:   344.39 ms (290,366 rows/s, 3.4 µs/row)
Round 5:   348.80 ms (286,695 rows/s, 3.5 µs/row)
Avg:       345.95 ms (289,060 rows/s, 3.5 µs/row)
```

Repeat runs of the same configuration produced averages within approximately 2% of these results.

| Schema | `clickhouse-connect` | `aiochlite` (tuples) | `aiochlite` (`Row`) | `aiochclient` |
| --- | ---: | ---: | ---: | ---: |
| flat columns | 150.54 ms | 153.74 ms | 180.91 ms | 334.24 ms |
| wide strings | 73.98 ms | 116.41 ms | 165.96 ms | 256.97 ms |
| nested containers | 124.02 ms | 162.27 ms | 194.20 ms | 345.95 ms |

Against `clickhouse-connect`, `aiochlite (tuples)` is level on flat columns (1.02x), 1.57x on wide strings and
1.31x on nested containers. All three schemas compile, so none of the gap is a fallback: it is what a
compiled Python loop costs against a C parser, and it widens as the share of per-value work grows. Strings
are the worst of the three because a `String` column is length-plus-bytes per row with nothing to batch,
where the flat schema's fixed-width columns go through one `struct` call.

Enabling compression on `aiochlite` closes little of it — 2.18x to 2.05x on a fetch of the wide-string schema
with no downstream work — so the difference is decoding, not transport.

The gap to `aiochlite (Row)` is the `Row` wrapper, one object per row, and it grows with column count: 18%-20%
on the four-column schemas, 43% on the ten-column one.

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
