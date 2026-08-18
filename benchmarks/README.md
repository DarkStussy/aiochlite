# Benchmarks

Three scripts, from the widest measurement to the narrowest:

- `fetch_rows.py` — end-to-end fetch + decode, against other clients.
- `decode_paths.py` — decoding alone: one reader call per column against the path a query takes now.
- `converter_cache.py` — one part of decoding: both sides of the value-cache trade.

> [!NOTE]
> The sample output was captured on a local machine with 8 CPU cores (16 threads) and 32 GB RAM, with ClickHouse
> 26.3 LTS on the same host. Compare the numbers within a run; absolute timings vary with CPU, kernel, server
> version and network.

## Methodology

- Every script prints a header with the versions it ran against, so a published result can be traced to what
  produced it.
- `gc` is off around every timed region.
- `fetch_rows.py` times the whole request and reports every round plus their average. The other two fetch the
  payload before any timing starts, so only decoding is measured, and report medians next to the raw series.
- When refreshing the results, update the measurement date in this file, then update the root
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
- Compares four clients, each in the configuration its own documentation recommends:
  - `aiochlite (Row)` — `AsyncChClient.fetch()`, in the client's default decode mode
  - `aiochlite (tuples)` — `AsyncChClient.fetch_rows()`, raw tuples
  - `clickhouse-connect (async)`
  - `aiochclient`

Competitor packages are deliberately absent from `pyproject.toml`. Install them separately, and give
`aiochclient` the `aiohttp-speedups` extra — without it date parsing falls back to pure Python and that client
runs roughly four times slower:

```bash
uv pip install "aiochclient[aiohttp-speedups]" clickhouse-connect
```

Run the script with `.venv/bin/python`, not `uv run`: `uv run` re-syncs the environment from `uv.lock` and
removes anything not declared there, competitors included.

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

Measured 2026-08-18.

```
Clients: aiochlite 1.7.1, aiochclient 2.7.0, clickhouse-connect 1.7.1
Python: 3.14.5, ClickHouse: 26.3.17.110
Rows: 100000, rounds: 10, warmup: 2

=== Schema: flat columns — id UInt64, event_time DateTime('UTC'), payload Tuple(String, UInt16), prices Array(Decimal(10, 2))
Table: bench_io_52f15aff036646eab85bdfb588fde010

IO benchmark (clickhouse-connect (async))
Round 1:    152.46 ms (655,902 rows/s, 1.5 µs/row)
Round 2:    150.63 ms (663,899 rows/s, 1.5 µs/row)
Round 3:    200.63 ms (498,441 rows/s, 2.0 µs/row)
Round 4:    150.66 ms (663,768 rows/s, 1.5 µs/row)
Round 5:    147.38 ms (678,540 rows/s, 1.5 µs/row)
Round 6:    149.26 ms (669,963 rows/s, 1.5 µs/row)
Round 7:    148.99 ms (671,208 rows/s, 1.5 µs/row)
Round 8:    149.79 ms (667,621 rows/s, 1.5 µs/row)
Round 9:    148.42 ms (673,755 rows/s, 1.5 µs/row)
Round 10:   148.77 ms (672,170 rows/s, 1.5 µs/row)
Avg:        154.70 ms (646,425 rows/s, 1.5 µs/row)

IO benchmark (aiochlite (Row))
Round 1:    183.77 ms (544,165 rows/s, 1.8 µs/row)
Round 2:    178.81 ms (559,258 rows/s, 1.8 µs/row)
Round 3:    182.35 ms (548,410 rows/s, 1.8 µs/row)
Round 4:    182.34 ms (548,420 rows/s, 1.8 µs/row)
Round 5:    178.22 ms (561,104 rows/s, 1.8 µs/row)
Round 6:    178.23 ms (561,062 rows/s, 1.8 µs/row)
Round 7:    182.15 ms (548,983 rows/s, 1.8 µs/row)
Round 8:    182.70 ms (547,331 rows/s, 1.8 µs/row)
Round 9:    182.26 ms (548,680 rows/s, 1.8 µs/row)
Round 10:   182.84 ms (546,940 rows/s, 1.8 µs/row)
Avg:        181.37 ms (551,369 rows/s, 1.8 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:    170.78 ms (585,552 rows/s, 1.7 µs/row)
Round 2:    167.95 ms (595,405 rows/s, 1.7 µs/row)
Round 3:    156.30 ms (639,812 rows/s, 1.6 µs/row)
Round 4:    157.79 ms (633,765 rows/s, 1.6 µs/row)
Round 5:    164.35 ms (608,446 rows/s, 1.6 µs/row)
Round 6:    161.12 ms (620,650 rows/s, 1.6 µs/row)
Round 7:    159.42 ms (627,270 rows/s, 1.6 µs/row)
Round 8:    159.99 ms (625,021 rows/s, 1.6 µs/row)
Round 9:    157.40 ms (635,310 rows/s, 1.6 µs/row)
Round 10:   156.52 ms (638,884 rows/s, 1.6 µs/row)
Avg:        161.16 ms (620,489 rows/s, 1.6 µs/row)

IO benchmark (aiochclient)
Round 1:    344.51 ms (290,269 rows/s, 3.4 µs/row)
Round 2:    343.78 ms (290,887 rows/s, 3.4 µs/row)
Round 3:    346.76 ms (288,386 rows/s, 3.5 µs/row)
Round 4:    344.80 ms (290,022 rows/s, 3.4 µs/row)
Round 5:    347.02 ms (288,170 rows/s, 3.5 µs/row)
Round 6:    343.69 ms (290,963 rows/s, 3.4 µs/row)
Round 7:    349.10 ms (286,449 rows/s, 3.5 µs/row)
Round 8:    347.01 ms (288,175 rows/s, 3.5 µs/row)
Round 9:    352.69 ms (283,532 rows/s, 3.5 µs/row)
Round 10:   347.16 ms (288,049 rows/s, 3.5 µs/row)
Avg:        346.65 ms (288,474 rows/s, 3.5 µs/row)

=== Schema: wide strings — id UInt64, s0 String, s1 String, s2 String, s3 String, s4 String, s5 String, s6 String, s7 String, s8 String
Table: bench_io_b6ee7aad75514212a9ccb1effaaee9ae

IO benchmark (clickhouse-connect (async))
Round 1:     80.24 ms (1,246,215 rows/s, 0.8 µs/row)
Round 2:     78.74 ms (1,269,973 rows/s, 0.8 µs/row)
Round 3:     75.51 ms (1,324,321 rows/s, 0.8 µs/row)
Round 4:     79.75 ms (1,253,924 rows/s, 0.8 µs/row)
Round 5:     73.12 ms (1,367,610 rows/s, 0.7 µs/row)
Round 6:     76.71 ms (1,303,658 rows/s, 0.8 µs/row)
Round 7:     77.76 ms (1,286,087 rows/s, 0.8 µs/row)
Round 8:     75.41 ms (1,326,033 rows/s, 0.8 µs/row)
Round 9:     76.26 ms (1,311,324 rows/s, 0.8 µs/row)
Round 10:    75.41 ms (1,325,998 rows/s, 0.8 µs/row)
Avg:         76.89 ms (1,300,535 rows/s, 0.8 µs/row)

IO benchmark (aiochlite (Row))
Round 1:    156.39 ms (639,433 rows/s, 1.6 µs/row)
Round 2:    156.15 ms (640,422 rows/s, 1.6 µs/row)
Round 3:    156.04 ms (640,843 rows/s, 1.6 µs/row)
Round 4:    152.30 ms (656,589 rows/s, 1.5 µs/row)
Round 5:    151.23 ms (661,265 rows/s, 1.5 µs/row)
Round 6:    159.23 ms (628,009 rows/s, 1.6 µs/row)
Round 7:    167.06 ms (598,597 rows/s, 1.7 µs/row)
Round 8:    156.14 ms (640,455 rows/s, 1.6 µs/row)
Round 9:    155.34 ms (643,762 rows/s, 1.6 µs/row)
Round 10:   156.81 ms (637,702 rows/s, 1.6 µs/row)
Avg:        156.67 ms (638,289 rows/s, 1.6 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:    119.12 ms (839,521 rows/s, 1.2 µs/row)
Round 2:    119.78 ms (834,885 rows/s, 1.2 µs/row)
Round 3:    120.42 ms (830,394 rows/s, 1.2 µs/row)
Round 4:    121.73 ms (821,468 rows/s, 1.2 µs/row)
Round 5:    119.21 ms (838,826 rows/s, 1.2 µs/row)
Round 6:    118.74 ms (842,142 rows/s, 1.2 µs/row)
Round 7:    124.78 ms (801,436 rows/s, 1.2 µs/row)
Round 8:    121.46 ms (823,283 rows/s, 1.2 µs/row)
Round 9:    119.28 ms (838,365 rows/s, 1.2 µs/row)
Round 10:   118.53 ms (843,663 rows/s, 1.2 µs/row)
Avg:        120.31 ms (831,213 rows/s, 1.2 µs/row)

IO benchmark (aiochclient)
Round 1:    268.35 ms (372,642 rows/s, 2.7 µs/row)
Round 2:    268.44 ms (372,529 rows/s, 2.7 µs/row)
Round 3:    279.11 ms (358,284 rows/s, 2.8 µs/row)
Round 4:    276.32 ms (361,901 rows/s, 2.8 µs/row)
Round 5:    274.14 ms (364,777 rows/s, 2.7 µs/row)
Round 6:    273.02 ms (366,270 rows/s, 2.7 µs/row)
Round 7:    274.90 ms (363,765 rows/s, 2.7 µs/row)
Round 8:    267.37 ms (374,014 rows/s, 2.7 µs/row)
Round 9:    269.36 ms (371,252 rows/s, 2.7 µs/row)
Round 10:   268.03 ms (373,096 rows/s, 2.7 µs/row)
Avg:        271.90 ms (367,777 rows/s, 2.7 µs/row)

=== Schema: nested containers — id UInt64, nested Array(Array(UInt8)), tags Map(String, Array(UInt8)), opt Array(Nullable(UInt64))
Table: bench_io_470a8b14ff7147afb131f8a95adbc014

IO benchmark (clickhouse-connect (async))
Round 1:    131.26 ms (761,874 rows/s, 1.3 µs/row)
Round 2:    131.64 ms (759,638 rows/s, 1.3 µs/row)
Round 3:    131.20 ms (762,166 rows/s, 1.3 µs/row)
Round 4:    133.53 ms (748,913 rows/s, 1.3 µs/row)
Round 5:    125.81 ms (794,880 rows/s, 1.3 µs/row)
Round 6:    130.11 ms (768,552 rows/s, 1.3 µs/row)
Round 7:    122.97 ms (813,189 rows/s, 1.2 µs/row)
Round 8:    130.07 ms (768,804 rows/s, 1.3 µs/row)
Round 9:    121.55 ms (822,739 rows/s, 1.2 µs/row)
Round 10:   130.22 ms (767,943 rows/s, 1.3 µs/row)
Avg:        128.84 ms (776,182 rows/s, 1.3 µs/row)

IO benchmark (aiochlite (Row))
Round 1:    188.87 ms (529,473 rows/s, 1.9 µs/row)
Round 2:    188.74 ms (529,825 rows/s, 1.9 µs/row)
Round 3:    192.74 ms (518,831 rows/s, 1.9 µs/row)
Round 4:    187.39 ms (533,638 rows/s, 1.9 µs/row)
Round 5:    191.15 ms (523,141 rows/s, 1.9 µs/row)
Round 6:    190.15 ms (525,894 rows/s, 1.9 µs/row)
Round 7:    194.31 ms (514,637 rows/s, 1.9 µs/row)
Round 8:    194.24 ms (514,819 rows/s, 1.9 µs/row)
Round 9:    213.50 ms (468,377 rows/s, 2.1 µs/row)
Round 10:   201.79 ms (495,576 rows/s, 2.0 µs/row)
Avg:        194.29 ms (514,697 rows/s, 1.9 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:    192.53 ms (519,401 rows/s, 1.9 µs/row)
Round 2:    169.86 ms (588,720 rows/s, 1.7 µs/row)
Round 3:    168.25 ms (594,341 rows/s, 1.7 µs/row)
Round 4:    167.61 ms (596,634 rows/s, 1.7 µs/row)
Round 5:    169.21 ms (590,998 rows/s, 1.7 µs/row)
Round 6:    163.06 ms (613,279 rows/s, 1.6 µs/row)
Round 7:    167.68 ms (596,365 rows/s, 1.7 µs/row)
Round 8:    167.31 ms (597,684 rows/s, 1.7 µs/row)
Round 9:    167.37 ms (597,462 rows/s, 1.7 µs/row)
Round 10:   164.80 ms (606,814 rows/s, 1.6 µs/row)
Avg:        169.77 ms (589,040 rows/s, 1.7 µs/row)

IO benchmark (aiochclient)
Round 1:    354.54 ms (282,058 rows/s, 3.5 µs/row)
Round 2:    357.66 ms (279,598 rows/s, 3.6 µs/row)
Round 3:    352.22 ms (283,911 rows/s, 3.5 µs/row)
Round 4:    360.94 ms (277,051 rows/s, 3.6 µs/row)
Round 5:    361.11 ms (276,924 rows/s, 3.6 µs/row)
Round 6:    358.51 ms (278,936 rows/s, 3.6 µs/row)
Round 7:    362.13 ms (276,142 rows/s, 3.6 µs/row)
Round 8:    351.71 ms (284,328 rows/s, 3.5 µs/row)
Round 9:    358.93 ms (278,606 rows/s, 3.6 µs/row)
Round 10:   380.26 ms (262,980 rows/s, 3.8 µs/row)
Avg:        359.80 ms (277,932 rows/s, 3.6 µs/row)
```

The sample was taken with `BENCH_ROUNDS=10`; the default 5 gives the same picture with noisier averages.
Repeat runs of the same configuration landed within about 5% of these averages, except for the `Row`-to-tuples
gap on the ten-column schema, which moved between 22% and 37% across runs.

| Schema | `clickhouse-connect` | `aiochlite` (tuples) | `aiochlite` (`Row`) | `aiochclient` |
| --- | ---: | ---: | ---: | ---: |
| flat columns | 154.70 ms | 161.16 ms | 181.37 ms | 346.65 ms |
| wide strings | 76.89 ms | 120.31 ms | 156.67 ms | 271.90 ms |
| nested containers | 128.84 ms | 169.77 ms | 194.29 ms | 359.80 ms |

Against `clickhouse-connect`, `aiochlite (tuples)` is close on flat columns (1.04x), 1.56x on wide strings and
1.32x on nested containers. All three schemas compile, so none of the gap is a fallback: it is what a compiled
Python loop costs against a C parser, and it widens as the share of per-value work grows. Strings are the worst
of the three, because a `String` column is a length plus bytes per row with nothing to batch, where the flat
schema's fixed-width columns go through one `struct` call.

Compression barely moves it: with `enable_compression=True` the wide-string ratio went from 2.24x to 2.17x. That
one was measured separately, on a fetch with no downstream work, and is not part of the output above. What
separates the two clients is decoding, not transport.

The gap to `aiochlite (Row)` is the `Row` wrapper itself, one object per row, and it grows with column count:
13% and 14% on the four-column schemas, 30% on the ten-column one. That last figure is the least stable number
here, so treat it as a range rather than a point.

## Decoder benchmark: per-field reads vs the current path

Script: `benchmarks/decode_paths.py`

What it measures:
- Row decoding alone. The payload is fetched once before any timing, so only bytes already in memory are
  decoded.
- The slow side keeps one reader call per column per row — what a schema falls back to when nothing in it can
  be emitted inline. The fast side is whatever `parse_rowbinary_with_names_and_types` picks for the same types:
  one `struct` pass over the whole body for a row that is fixed-width end to end, otherwise a loop compiled for
  the schema.
- Three schemas, chosen so the converters land differently: fixed-width end to end with one `DateTime`, a mixed
  row with a `String` in the middle, and a wide numeric row with no converter at all.
- A sweep of row width at parity: N consecutive `UInt64` columns for N = 2..10.
- Both decoders are rebuilt every round with the module caches cleared. Reusing them leaves the converter cache
  warm from the round before, which reads as a fast decode where a single query would have only misses.

Run:

```bash
.venv/bin/python benchmarks/decode_paths.py
```

Same environment variables as above, with different defaults — `BENCH_ROWS=200000`, `BENCH_ROUNDS=7`,
`BENCH_WARMUP=2` — plus `BENCH_SWEEP_ROWS` (default `100000`) for the width sweep, which runs nine schemas and
would otherwise stretch the run.

The sample below was taken with `BENCH_ROUNDS=11`; the default 7 gives the same shape with noisier medians on
the sweep.

### Sample output

Measured 2026-08-18.

```
aiochlite 1.7.0 @ 31e76b0
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Rows: 200000, rounds: 11, warmup: 2
Sweep rows: 100000
ClickHouse: 26.3.17.110

Fully fixed-width row (5 columns) — one struct pass over the body
  Per field: median  199.18 ms  [201.13, 197.41, 199.18, 205.69, 204.07, 196.38, 198.24, 201.29, 230.62, 198.12, 196.43]
  Current:   median  108.28 ms  [115.65, 109.74, 108.21, 107.16, 108.28, 109.36, 111.11, 112.32, 106.72, 107.72, 107.72]
  Speedup:   1.84x

Mixed row, one run of 3 (5 columns) — loop compiled for the schema
  Per field: median  231.60 ms  [236.79, 231.41, 233.01, 230.09, 234.50, 243.80, 246.04, 230.12, 231.60, 225.72, 225.65]
  Current:   median  133.23 ms  [133.23, 135.97, 132.00, 129.74, 131.77, 134.19, 134.45, 135.75, 135.22, 131.96, 130.56]
  Speedup:   1.74x

Wide numeric row (10 columns) — one struct pass over the body
  Per field: median  228.74 ms  [234.32, 228.96, 231.51, 232.84, 228.74, 223.67, 225.58, 229.42, 224.28, 221.58, 220.94]
  Current:   median   27.67 ms  [28.92, 28.76, 29.77, 28.75, 34.33, 27.67, 26.70, 26.87, 25.99, 26.10, 25.55]
  Speedup:   8.27x

Row width at parity, 100000 rows of UInt64 columns

2 columns
  Per field: median   24.74 ms  [24.74, 26.36, 24.38, 24.80, 24.48, 24.84, 24.68, 25.34, 24.48, 33.72, 23.74]
  Current:   median    3.74 ms  [3.72, 3.86, 3.66, 3.67, 3.74, 3.87, 3.73, 3.78, 3.76, 3.72, 3.90]
  Speedup:   6.62x

...

10 columns
  Per field: median  104.17 ms  [110.29, 104.56, 113.02, 103.10, 104.17, 106.26, 106.69, 102.93, 101.24, 101.24, 102.84]
  Current:   median    8.84 ms  [9.58, 9.44, 8.84, 8.94, 8.34, 9.15, 7.79, 9.00, 8.32, 7.64, 7.77]
  Speedup:   11.79x
```

The sweep prints a block per width; the ones between 2 and 10 are cut here for length. Their medians:

| Columns | Per field | Current | Speedup |
|---:|---:|---:|---:|
| 3 | 34.62 ms | 4.12 ms | 8.40x |
| 4 | 42.05 ms | 4.46 ms | 9.43x |
| 5 | 52.30 ms | 5.23 ms | 10.00x |
| 6 | 62.61 ms | 5.56 ms | 11.26x |
| 7 | 72.22 ms | 6.38 ms | 11.33x |
| 8 | 81.07 ms | 6.91 ms | 11.73x |
| 9 | 93.18 ms | 7.84 ms | 11.89x |

Reading by field grows about 10 ms per added column, which is what a call chain per cell costs. The current path
grows about 0.6 ms: `iter_unpack` still builds one Python object per field, and no amount of batching removes
that. The ratio therefore climbs and then flattens out around 12x rather than tracking column count.

The five-column schemas show why one number is not enough. Fixed-width end to end gives 1.84x, the sweep at the
same width gives 10.00x — the difference is a single `DateTime`, whose converter stays on both paths and dilutes
everything else.

## Converter benchmark: the value cache

Script: `benchmarks/converter_cache.py`

What it measures:
- Both decoders come from `_row_decoder`, the compiled path a query takes for this schema, so the conversion is
  measured inside the decode that surrounds it in a real query.
- Fixed-width columns such as `DateTime` and `Decimal` arrive as integers and are turned into Python objects by a
  converter. Converters built per query memoize through `_value_cache`, which holds up to
  `_QUERY_VALUE_CACHE_SIZE` values per column and is released with the query. Converters reached through
  `_reader_for_type` outlive the query that built them and keep their own bound (`_VALUE_CACHE_SIZE`).
- The same schema is decoded twice, by a row reader whose converters memoize and by one whose converters do not.
  The uncached variant is built by switching `_value_cache` off while the converters are created, so both readers
  run identical conversion logic; the script checks that the patch took effect and that both decoders agree.
- Two payloads: one where values repeat heavily, one where every value is distinct.
- The agreement check sits outside the timer, the decoders take turns going first, and each result is released
  before the next round.

Run:

```bash
.venv/bin/python benchmarks/converter_cache.py
```

Same environment variables as the script above, minus the sweep: `BENCH_ROWS` defaults to `200000`,
`BENCH_ROUNDS` to `7`, `BENCH_WARMUP` to `2`.

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

Both sides of the trade are large here: repeated values make the cache save 80%, all-distinct values make it
cost 44%. Two of the three columns go through a converter, so conversion dominates the decode; on a wide schema
where one column in ten is a `DateTime` both numbers shrink. Measure it rather than assume it.

The decoders are rebuilt every round on purpose. Share one across rounds and its cache stays warm from the round
before, so the high-cardinality case reports hits where a single query would have only misses — it read as a 70%
saving instead of a cost.

Neither payload covers the middle ground between them, and that is what decided the cache's shape. Three ways to
bound it, on 300k rows of the schema above, against 174 ms with no cache at all:

| Distinct values | Order | `lru_cache` | Fill and stop | Fill and start over |
| --- | --- | ---: | ---: | ---: |
| 20,000 | random | 224 ms | 52 ms | 51 ms |
| 100,000 | random | — | 152 ms | 252 ms |
| 100,000 | sorted | — | 150 ms | 110 ms |
| 100,000 then 100 new hot ones | — | — | 235 ms | 108 ms |

Eviction collapses: at 20k distinct values a 4096-entry `lru_cache` is slower than no cache at all, every lookup
missing and every insert evicting. Filling and stopping avoids that, but it keeps whatever it saw first and so
misses forever once the working set moves — the last two rows, which are what a column in time order looks like.
Starting over loses only where access is uniformly random over a cardinality just above the bound.
`_QUERY_VALUE_CACHE_SIZE` takes that last policy. None of these four rows is in the script: they were measured
by hand, and each still deserves a payload there.

The bound is also what keeps `stream()` flat: there the cache lives as long as the query, while the caller drops
each row as it goes. Peak traced memory:

| Rows | Unbounded | Bounded |
| --- | ---: | ---: |
| 100,000 | 36.6 MiB | 23.4 MiB |
| 1,000,000 | 297.4 MiB | 23.6 MiB |
| 3,000,000 | 1033.3 MiB | 24.4 MiB |
