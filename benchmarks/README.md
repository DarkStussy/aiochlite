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

Measured 2026-08-18, on `main` past the 1.7.0 tag: the header prints the version in
`pyproject.toml`, which is the last released one.

```
Clients: aiochlite 1.7.0, aiochclient 2.7.0, clickhouse-connect 1.7.1
Python: 3.14.5, ClickHouse: 26.3.17.110
Rows: 100000, rounds: 10, warmup: 2

=== Schema: flat columns — id UInt64, event_time DateTime('UTC'), payload Tuple(String, UInt16), prices Array(Decimal(10, 2))
Table: bench_io_7a5a250ec6004417b0a821265a1d5e3d

IO benchmark (clickhouse-connect (async))
Round 1:    152.89 ms (654,077 rows/s, 1.5 µs/row)
Round 2:    152.11 ms (657,440 rows/s, 1.5 µs/row)
Round 3:    148.92 ms (671,506 rows/s, 1.5 µs/row)
Round 4:    148.91 ms (671,532 rows/s, 1.5 µs/row)
Round 5:    149.22 ms (670,145 rows/s, 1.5 µs/row)
Round 6:    148.65 ms (672,706 rows/s, 1.5 µs/row)
Round 7:    146.34 ms (683,341 rows/s, 1.5 µs/row)
Round 8:    148.86 ms (671,763 rows/s, 1.5 µs/row)
Round 9:    145.15 ms (688,925 rows/s, 1.5 µs/row)
Round 10:   157.86 ms (633,478 rows/s, 1.6 µs/row)
Avg:        149.89 ms (667,150 rows/s, 1.5 µs/row)

IO benchmark (aiochlite (Row))
Round 1:    174.55 ms (572,903 rows/s, 1.7 µs/row)
Round 2:    177.66 ms (562,860 rows/s, 1.8 µs/row)
Round 3:    178.33 ms (560,743 rows/s, 1.8 µs/row)
Round 4:    173.69 ms (575,752 rows/s, 1.7 µs/row)
Round 5:    177.53 ms (563,294 rows/s, 1.8 µs/row)
Round 6:    174.93 ms (571,652 rows/s, 1.7 µs/row)
Round 7:    180.20 ms (554,929 rows/s, 1.8 µs/row)
Round 8:    175.86 ms (568,638 rows/s, 1.8 µs/row)
Round 9:    181.90 ms (549,742 rows/s, 1.8 µs/row)
Round 10:   174.15 ms (574,226 rows/s, 1.7 µs/row)
Avg:        176.88 ms (565,353 rows/s, 1.8 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:    165.79 ms (603,157 rows/s, 1.7 µs/row)
Round 2:    160.07 ms (624,719 rows/s, 1.6 µs/row)
Round 3:    162.52 ms (615,328 rows/s, 1.6 µs/row)
Round 4:    159.70 ms (626,155 rows/s, 1.6 µs/row)
Round 5:    158.91 ms (629,298 rows/s, 1.6 µs/row)
Round 6:    154.64 ms (646,682 rows/s, 1.5 µs/row)
Round 7:    155.66 ms (642,424 rows/s, 1.6 µs/row)
Round 8:    156.87 ms (637,486 rows/s, 1.6 µs/row)
Round 9:    159.24 ms (628,000 rows/s, 1.6 µs/row)
Round 10:   157.89 ms (633,338 rows/s, 1.6 µs/row)
Avg:        159.13 ms (628,423 rows/s, 1.6 µs/row)

IO benchmark (aiochclient)
Round 1:    339.68 ms (294,398 rows/s, 3.4 µs/row)
Round 2:    341.09 ms (293,179 rows/s, 3.4 µs/row)
Round 3:    349.91 ms (285,787 rows/s, 3.5 µs/row)
Round 4:    337.74 ms (296,082 rows/s, 3.4 µs/row)
Round 5:    341.54 ms (292,788 rows/s, 3.4 µs/row)
Round 6:    344.36 ms (290,391 rows/s, 3.4 µs/row)
Round 7:    343.95 ms (290,741 rows/s, 3.4 µs/row)
Round 8:    346.48 ms (288,615 rows/s, 3.5 µs/row)
Round 9:    336.72 ms (296,985 rows/s, 3.4 µs/row)
Round 10:   335.85 ms (297,752 rows/s, 3.4 µs/row)
Avg:        341.73 ms (292,626 rows/s, 3.4 µs/row)

=== Schema: wide strings — id UInt64, s0 String, s1 String, s2 String, s3 String, s4 String, s5 String, s6 String, s7 String, s8 String
Table: bench_io_0e5bb38cfe3745a0aeaf272f75738dfd

IO benchmark (clickhouse-connect (async))
Round 1:     77.93 ms (1,283,252 rows/s, 0.8 µs/row)
Round 2:     91.38 ms (1,094,284 rows/s, 0.9 µs/row)
Round 3:     72.87 ms (1,372,214 rows/s, 0.7 µs/row)
Round 4:     77.46 ms (1,291,030 rows/s, 0.8 µs/row)
Round 5:     71.21 ms (1,404,395 rows/s, 0.7 µs/row)
Round 6:     81.32 ms (1,229,707 rows/s, 0.8 µs/row)
Round 7:     71.16 ms (1,405,251 rows/s, 0.7 µs/row)
Round 8:     81.77 ms (1,222,939 rows/s, 0.8 µs/row)
Round 9:     71.12 ms (1,406,054 rows/s, 0.7 µs/row)
Round 10:    74.91 ms (1,334,916 rows/s, 0.7 µs/row)
Avg:         77.11 ms (1,296,794 rows/s, 0.8 µs/row)

IO benchmark (aiochlite (Row))
Round 1:    147.94 ms (675,939 rows/s, 1.5 µs/row)
Round 2:    150.93 ms (662,551 rows/s, 1.5 µs/row)
Round 3:    149.81 ms (667,490 rows/s, 1.5 µs/row)
Round 4:    151.38 ms (660,606 rows/s, 1.5 µs/row)
Round 5:    150.02 ms (666,572 rows/s, 1.5 µs/row)
Round 6:    152.65 ms (655,105 rows/s, 1.5 µs/row)
Round 7:    155.20 ms (644,342 rows/s, 1.6 µs/row)
Round 8:    149.17 ms (670,398 rows/s, 1.5 µs/row)
Round 9:    150.36 ms (665,084 rows/s, 1.5 µs/row)
Round 10:   149.32 ms (669,689 rows/s, 1.5 µs/row)
Avg:        150.68 ms (663,669 rows/s, 1.5 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:    115.75 ms (863,957 rows/s, 1.2 µs/row)
Round 2:    120.73 ms (828,290 rows/s, 1.2 µs/row)
Round 3:    118.02 ms (847,283 rows/s, 1.2 µs/row)
Round 4:    128.29 ms (779,481 rows/s, 1.3 µs/row)
Round 5:    120.95 ms (826,773 rows/s, 1.2 µs/row)
Round 6:    117.87 ms (848,387 rows/s, 1.2 µs/row)
Round 7:    115.75 ms (863,944 rows/s, 1.2 µs/row)
Round 8:    115.49 ms (865,885 rows/s, 1.2 µs/row)
Round 9:    115.57 ms (865,265 rows/s, 1.2 µs/row)
Round 10:   116.37 ms (859,352 rows/s, 1.2 µs/row)
Avg:        118.48 ms (844,031 rows/s, 1.2 µs/row)

IO benchmark (aiochclient)
Round 1:    265.57 ms (376,542 rows/s, 2.7 µs/row)
Round 2:    261.32 ms (382,671 rows/s, 2.6 µs/row)
Round 3:    261.00 ms (383,148 rows/s, 2.6 µs/row)
Round 4:    264.16 ms (378,559 rows/s, 2.6 µs/row)
Round 5:    260.86 ms (383,353 rows/s, 2.6 µs/row)
Round 6:    262.15 ms (381,468 rows/s, 2.6 µs/row)
Round 7:    267.41 ms (373,954 rows/s, 2.7 µs/row)
Round 8:    261.96 ms (381,734 rows/s, 2.6 µs/row)
Round 9:    260.40 ms (384,018 rows/s, 2.6 µs/row)
Round 10:   262.26 ms (381,298 rows/s, 2.6 µs/row)
Avg:        262.71 ms (380,649 rows/s, 2.6 µs/row)

=== Schema: nested containers — id UInt64, nested Array(Array(UInt8)), tags Map(String, Array(UInt8)), opt Array(Nullable(UInt64))
Table: bench_io_d574f825a8ca4a4cb30471407eb669c1

IO benchmark (clickhouse-connect (async))
Round 1:    128.99 ms (775,262 rows/s, 1.3 µs/row)
Round 2:    132.88 ms (752,579 rows/s, 1.3 µs/row)
Round 3:    131.11 ms (762,708 rows/s, 1.3 µs/row)
Round 4:    130.89 ms (764,022 rows/s, 1.3 µs/row)
Round 5:    121.25 ms (824,712 rows/s, 1.2 µs/row)
Round 6:    127.03 ms (787,228 rows/s, 1.3 µs/row)
Round 7:    123.39 ms (810,451 rows/s, 1.2 µs/row)
Round 8:    130.95 ms (763,663 rows/s, 1.3 µs/row)
Round 9:    126.25 ms (792,091 rows/s, 1.3 µs/row)
Round 10:   129.31 ms (773,343 rows/s, 1.3 µs/row)
Avg:        128.20 ms (780,008 rows/s, 1.3 µs/row)

IO benchmark (aiochlite (Row))
Round 1:    188.40 ms (530,776 rows/s, 1.9 µs/row)
Round 2:    185.62 ms (538,732 rows/s, 1.9 µs/row)
Round 3:    186.73 ms (535,542 rows/s, 1.9 µs/row)
Round 4:    191.45 ms (522,331 rows/s, 1.9 µs/row)
Round 5:    189.58 ms (527,476 rows/s, 1.9 µs/row)
Round 6:    203.48 ms (491,445 rows/s, 2.0 µs/row)
Round 7:    192.52 ms (519,430 rows/s, 1.9 µs/row)
Round 8:    192.96 ms (518,234 rows/s, 1.9 µs/row)
Round 9:    193.91 ms (515,708 rows/s, 1.9 µs/row)
Round 10:   195.06 ms (512,672 rows/s, 2.0 µs/row)
Avg:        191.97 ms (520,912 rows/s, 1.9 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:    165.48 ms (604,300 rows/s, 1.7 µs/row)
Round 2:    164.50 ms (607,900 rows/s, 1.6 µs/row)
Round 3:    163.46 ms (611,768 rows/s, 1.6 µs/row)
Round 4:    164.85 ms (606,624 rows/s, 1.6 µs/row)
Round 5:    161.91 ms (617,611 rows/s, 1.6 µs/row)
Round 6:    166.32 ms (601,243 rows/s, 1.7 µs/row)
Round 7:    161.38 ms (619,653 rows/s, 1.6 µs/row)
Round 8:    168.63 ms (593,006 rows/s, 1.7 µs/row)
Round 9:    165.74 ms (603,338 rows/s, 1.7 µs/row)
Round 10:   164.27 ms (608,743 rows/s, 1.6 µs/row)
Avg:        164.66 ms (607,329 rows/s, 1.6 µs/row)

IO benchmark (aiochclient)
Round 1:    354.92 ms (281,751 rows/s, 3.5 µs/row)
Round 2:    365.82 ms (273,355 rows/s, 3.7 µs/row)
Round 3:    351.50 ms (284,497 rows/s, 3.5 µs/row)
Round 4:    359.79 ms (277,940 rows/s, 3.6 µs/row)
Round 5:    362.16 ms (276,123 rows/s, 3.6 µs/row)
Round 6:    364.42 ms (274,406 rows/s, 3.6 µs/row)
Round 7:    352.83 ms (283,420 rows/s, 3.5 µs/row)
Round 8:    362.75 ms (275,671 rows/s, 3.6 µs/row)
Round 9:    350.13 ms (285,612 rows/s, 3.5 µs/row)
Round 10:   354.98 ms (281,705 rows/s, 3.5 µs/row)
Avg:        357.93 ms (279,384 rows/s, 3.6 µs/row)
```

The sample was taken with `BENCH_ROUNDS=10`; the default 5 gives the same picture with noisier averages.
Repeat runs of the same configuration landed within about 5% of these averages, except for the `Row`-to-tuples
gap on the ten-column schema, which moved between 22% and 37% across runs.

| Schema | `clickhouse-connect` | `aiochlite` (tuples) | `aiochlite` (`Row`) | `aiochclient` |
| --- | ---: | ---: | ---: | ---: |
| flat columns | 149.89 ms | 159.13 ms | 176.88 ms | 341.73 ms |
| wide strings | 77.11 ms | 118.48 ms | 150.68 ms | 262.71 ms |
| nested containers | 128.20 ms | 164.66 ms | 191.97 ms | 357.93 ms |

Against `clickhouse-connect`, `aiochlite (tuples)` is close on flat columns (1.06x), 1.54x on wide strings and
1.28x on nested containers. All three schemas compile, so none of the gap is a fallback: it is what a compiled
Python loop costs against a C parser, and it widens as the share of per-value work grows. Strings are the worst
of the three, because a `String` column is a length plus bytes per row with nothing to batch, where the flat
schema's fixed-width columns go through one `struct` call.

Compression barely moves it: with `enable_compression=True` the wide-string ratio went from 2.24x to 2.17x. That
one was measured separately, on a fetch with no downstream work, and is not part of the output above. What
separates the two clients is decoding, not transport.

The gap to `aiochlite (Row)` is the `Row` wrapper itself, one object per row, and it grows with column count:
11% and 17% on the four-column schemas, 27% on the ten-column one. That last figure is the least stable number
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
