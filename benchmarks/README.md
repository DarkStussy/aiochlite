# Benchmarks

This directory contains benchmark scripts for `aiochlite`:

- `fetch_rows.py` — end-to-end fetch + decode, compared against other clients.
- `decode_fusion.py` — decoding only, comparing two decoders inside `aiochlite` itself.
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

Measured 2026-08-14.

```
Clients: aiochlite 1.4.0, aiochclient 2.7.0, clickhouse-connect 1.7.1
Python: 3.14.5, ClickHouse: 26.3.17.110
Rows: 100000, rounds: 5, warmup: 2
Table: bench_io_07b81969c44742fc9840fbef04c9c0b1

IO benchmark (clickhouse-connect (async))
Round 1:   160.30 ms (623,843 rows/s, 1.6 µs/row)
Round 2:   162.90 ms (613,876 rows/s, 1.6 µs/row)
Round 3:   159.24 ms (627,982 rows/s, 1.6 µs/row)
Round 4:   159.96 ms (625,142 rows/s, 1.6 µs/row)
Round 5:   163.55 ms (611,442 rows/s, 1.6 µs/row)
Avg:       161.19 ms (620,388 rows/s, 1.6 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   303.78 ms (329,189 rows/s, 3.0 µs/row)
Round 2:   304.48 ms (328,433 rows/s, 3.0 µs/row)
Round 3:   303.58 ms (329,407 rows/s, 3.0 µs/row)
Round 4:   301.80 ms (331,348 rows/s, 3.0 µs/row)
Round 5:   303.44 ms (329,555 rows/s, 3.0 µs/row)
Avg:       303.41 ms (329,584 rows/s, 3.0 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   275.18 ms (363,400 rows/s, 2.8 µs/row)
Round 2:   275.40 ms (363,113 rows/s, 2.8 µs/row)
Round 3:   277.56 ms (360,286 rows/s, 2.8 µs/row)
Round 4:   273.58 ms (365,523 rows/s, 2.7 µs/row)
Round 5:   277.22 ms (360,721 rows/s, 2.8 µs/row)
Avg:       275.79 ms (362,598 rows/s, 2.8 µs/row)

IO benchmark (aiochclient)
Round 1:   361.52 ms (276,613 rows/s, 3.6 µs/row)
Round 2:   356.55 ms (280,469 rows/s, 3.6 µs/row)
Round 3:   370.12 ms (270,183 rows/s, 3.7 µs/row)
Round 4:   364.13 ms (274,625 rows/s, 3.6 µs/row)
Round 5:   363.73 ms (274,929 rows/s, 3.6 µs/row)
Avg:       363.21 ms (275,324 rows/s, 3.6 µs/row)
```

Repeat runs of the same configuration produced averages within approximately 2% of these results.

## Decoder benchmark: fixed-width fusion

Script: `benchmarks/decode_fusion.py`

What it measures:
- Decoding only. The payload is fetched once, before any timing starts, so neither the network nor the server
  appears in the measurement.
- Compares two decoders inside `aiochlite` over the same in-memory `RowBinaryWithNamesAndTypes` payload:
  - `per-field`: one reader call per column.
  - `fused`: runs of consecutive fixed-width columns collapsed into a single `struct` call.
- Three representative schemas (fully fixed-width, mixed, wide numeric), then a sweep of 2 to 10 consecutive
  `UInt64` columns that isolates run length from everything else.
- Needs no competitor packages, so it runs against the project environment as is.

Method:
- Both decoders are checked for identical decoded output once, before any timing.
- Rounds alternate which decoder runs first, so drift over the run is shared rather than charged to one side.
- Each decoded result is released before the next measurement starts, so only one result is ever live.
- Medians are reported next to the raw series; a mean would hide how noisy these numbers are.

Run:

```bash
.venv/bin/python benchmarks/decode_fusion.py
```

Environment variables are the same as above, plus `BENCH_SWEEP_ROWS` (default: `100000`). The defaults differ:
`BENCH_ROWS` is `200000` and `BENCH_ROUNDS` is `7`.

### Sample output

Measured 2026-08-15.

```
aiochlite 1.6.0 @ f14512b
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Rounds: 7, warmup: 2
ClickHouse: 26.3.17.110

=== Representative schemas (200,000 rows) ===

Fully fixed-width row (5 columns)
  Schema:    UInt64, UInt32, Float64, Int16, DateTime('UTC')
  Payload:   5.0 MiB, 200,000 rows
  Per-field: median  187.07 ms  [187.07, 194.24, 195.04, 188.86, 185.33, 185.99, 184.72]
  Fused:     median  123.60 ms  [124.87, 123.77, 124.94, 123.60, 122.36, 123.57, 121.92]
  Ratio:     1.51x (-33.9% time)

Mixed row, one fusable run of 3 (5 columns)
  Schema:    UInt64, String, DateTime('UTC'), Float64, Int32
  Payload:   5.9 MiB, 200,000 rows
  Per-field: median  220.22 ms  [219.87, 228.54, 221.85, 220.02, 220.22, 218.85, 221.88]
  Fused:     median  219.61 ms  [222.55, 220.27, 218.98, 219.37, 220.86, 219.61, 218.80]
  Ratio:     1.00x (-0.3% time)

Wide numeric row (10 columns)
  Schema:    UInt64, UInt64, UInt64, UInt64, UInt64, Float64, Float64, Float64, Float64, Float64
  Payload:   15.3 MiB, 200,000 rows
  Per-field: median  198.32 ms  [198.32, 198.82, 210.64, 197.15, 196.97, 206.01, 195.28]
  Fused:     median   55.15 ms  [54.99, 54.92, 56.42, 55.84, 55.15, 53.84, 57.36]
  Ratio:     3.60x (-72.2% time)

=== Fusion gain by run length (100,000 rows, UInt64 columns only) ===

  columns    per-field        fused   ratio
        2     22.07 ms     16.00 ms   1.38x
        3     29.47 ms     16.67 ms   1.77x
        4     38.07 ms     16.87 ms   2.26x
        5     48.29 ms     17.65 ms   2.74x
        6     55.41 ms     18.16 ms   3.05x
        7     64.55 ms     19.05 ms   3.39x
        8     72.53 ms     19.29 ms   3.76x
        9     84.94 ms     21.36 ms   3.98x
       10     94.13 ms     21.90 ms   4.30x
```

The sweep is the part worth reading, and the two growth rates are what reproduce across runs. Per-field decoding
grows roughly linearly with the column count — about 9 ms per column here — while the fused decoder grows by
roughly 1 ms per column, because a longer `struct` format still produces one more Python object per field. The
ratio follows from those two rates: it keeps climbing, and the curve is concave. The per-step increments are too
noisy to read individually; only the shape survives repeated runs.

On the mixed schema fusion buys nothing measurable. Across runs it lands on both sides of `1.00x`, which is the
honest reading: a single run of three columns among variable-width ones does not pay for its own segment call.
That is worth noting against `_MIN_SEGMENTED_FUSED_FIELDS = 3` — on this schema the threshold looks optimistic
rather than conservative.

## Converter benchmark: the value cache

Script: `benchmarks/converter_cache.py`

What it measures:
- Fixed-width columns such as `DateTime` and `Decimal` arrive as integers and are turned into Python objects by a
  converter. Those converters memoize with a bounded `lru_cache` (`_VALUE_CACHE_SIZE`, 4096 per converter).
- The same schema is decoded twice, by a row reader whose converters memoize and by one whose converters do not.
  The uncached variant is built by neutralizing `lru_cache` while the converters are created, so both readers run
  identical conversion logic; the script asserts the patch took effect and that both decoders agree.
- Two payloads: one where values repeat heavily, one where every value is distinct.
- Same timing method as `decode_fusion.py`: agreement check outside the timer, alternating order, previous result
  released before the next round, medians next to the raw series.

Run:

```bash
.venv/bin/python benchmarks/converter_cache.py
```

### Sample output

Measured 2026-08-15.

```
aiochlite 1.6.0 @ f14512b
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Schema: UInt64, DateTime('UTC'), Decimal(18, 2)
Rows: 200000, rounds: 7, warmup: 2, cache: 4096
ClickHouse: 26.3.17.110

Low cardinality — 200 distinct timestamps, 100 distinct prices
  Cached:   median   44.68 ms  [44.79, 45.25, 44.68, 44.58, 44.24, 44.65, 45.69]
  Uncached: median  150.55 ms  [151.54, 150.55, 150.91, 150.03, 150.15, 149.19, 151.01]
  Cache changes decode time by -70.3%

High cardinality — every timestamp and price distinct
  Cached:   median  169.37 ms  [169.37, 167.76, 169.87, 169.83, 168.06, 173.24, 168.87]
  Uncached: median  152.00 ms  [152.00, 152.07, 151.91, 151.22, 151.80, 154.33, 152.49]
  Cache changes decode time by +11.4%
```

On this schema an all-miss cache adds about 11% to decode time. The lookup is paid once per converted value,
so its absolute cost is fairly steady, but its share of the total depends on the rest of the schema. The benefit
side is not a constant either: it scales with how much of the decode is conversion. Two of the three columns here
go through a converter, so conversion dominates and the win reaches 70%. On a wide schema where one column in ten
is a `DateTime`, that share is likely smaller — measure it rather than assume it.

Measure it on your own data before treating the cache as free.
