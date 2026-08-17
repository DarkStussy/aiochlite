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

## Decoder benchmark: fixed-width fusion

Script: `benchmarks/decode_fusion.py`

> [!WARNING]
> This benchmark calls `_make_row_reader` directly, and no query reaches it any more. A column now either is
> emitted inline by the compiled decoder or read through its own closure, and fusion applies to exactly the
> columns the generator emits inline. Over 11,154 type combinations there is no schema where the compiled
> decoder declines the row and fusion still applies. Keep the numbers only as a record of what the path did.

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

Measured 2026-08-17.

```
aiochlite 1.6.0 @ f0434e0
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Rounds: 7, warmup: 2
ClickHouse: 26.3.17.110

=== Representative schemas (200,000 rows) ===

Fully fixed-width row (5 columns)
  Schema:    UInt64, UInt32, Float64, Int16, DateTime('UTC')
  Payload:   5.0 MiB, 200,000 rows
  Per-field: median  208.11 ms  [200.19, 198.97, 208.17, 207.66, 210.88, 208.11, 211.48]
  Fused:     median   40.24 ms  [39.58, 39.71, 40.92, 40.69, 40.13, 40.24, 40.27]
  Ratio:     5.17x (-80.7% time)

Mixed row, one fusable run of 3 (5 columns)
  Schema:    UInt64, String, DateTime('UTC'), Float64, Int32
  Payload:   5.9 MiB, 200,000 rows
  Per-field: median  235.43 ms  [231.15, 233.26, 234.74, 240.03, 235.43, 235.83, 241.00]
  Fused:     median  139.47 ms  [138.56, 140.18, 139.47, 152.13, 138.67, 139.26, 147.40]
  Ratio:     1.69x (-40.8% time)

Wide numeric row (10 columns)
  Schema:    UInt64, UInt64, UInt64, UInt64, UInt64, Float64, Float64, Float64, Float64, Float64
  Payload:   15.3 MiB, 200,000 rows
  Per-field: median  248.39 ms  [248.39, 245.56, 259.44, 254.95, 250.85, 247.36, 245.46]
  Fused:     median   53.88 ms  [54.93, 54.97, 55.23, 52.55, 53.88, 53.30, 51.55]
  Ratio:     4.61x (-78.3% time)

=== Fusion gain by run length (100,000 rows, UInt64 columns only) ===

  columns    per-field        fused   ratio
        2     26.83 ms     14.55 ms   1.84x
        3     37.99 ms     15.07 ms   2.52x
        4     47.18 ms     15.61 ms   3.02x
        5     58.75 ms     16.47 ms   3.57x
        6     68.98 ms     16.79 ms   4.11x
        7     79.48 ms     17.05 ms   4.66x
        8     91.75 ms     18.18 ms   5.05x
        9    104.69 ms     20.00 ms   5.23x
       10    115.15 ms     20.40 ms   5.65x
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
  converter. Converters built per query memoize through `_value_cache`, which holds every distinct value the query
  produced and is released with it. Only converters reached through `_reader_for_type`, which outlive the query
  that built them, keep a bound (`_VALUE_CACHE_SIZE`).
- The same schema is decoded twice, by a row reader whose converters memoize and by one whose converters do not.
  The uncached variant is built by neutralizing `_value_cache` while the converters are created, so both readers
  run identical conversion logic; the script asserts the patch took effect and that both decoders agree.
- Two payloads: one where values repeat heavily, one where every value is distinct.
- Same timing method as `decode_fusion.py`: agreement check outside the timer, alternating order, previous result
  released before the next round, medians next to the raw series.

Run:

```bash
.venv/bin/python benchmarks/converter_cache.py
```

### Sample output

Measured 2026-08-17.

```
aiochlite 1.6.0 @ f0434e0
CPU: AMD Ryzen 7 9800X3D 8-Core Processor
OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
Python: 3.14.5 (CPython)
Schema: UInt64, DateTime('UTC'), Decimal(18, 2)
Rows: 200000, rounds: 7, warmup: 2
ClickHouse: 26.3.17.110

Low cardinality — 200 distinct timestamps, 100 distinct prices
  Cached:   median   41.62 ms  [42.35, 41.62, 41.60, 41.46, 41.48, 41.93, 42.50]
  Uncached: median  145.04 ms  [145.54, 147.03, 145.20, 144.17, 143.57, 145.04, 143.41]
  Cache changes decode time by -71.3%

High cardinality — every timestamp and price distinct
  Cached:   median  195.52 ms  [192.45, 194.78, 193.96, 214.87, 195.64, 196.58, 195.52]
  Uncached: median  155.35 ms  [154.63, 155.83, 154.95, 159.36, 155.35, 155.12, 156.34]
  Cache changes decode time by +25.9%
```

The two sides of the trade are both large here: repeated values make the cache save 71%, all-distinct values
make it cost 26%. Two of the three columns go through a converter, so conversion dominates the decode; on a wide
schema where one column in ten is a `DateTime` both numbers shrink. Measure it rather than assume it.

The decoders are rebuilt every round on purpose. Sharing one across rounds leaves its cache warm from the round
before, and the high-cardinality case then reports hits where a single query has only misses — it read as a 70%
saving instead of a 26% cost.

Neither payload covers the case that decided the cache's shape: a cardinality just above the old bound, where
every lookup misses and every insert evicts. At 20k distinct, 200k `DateTime` values took 51.7 ms through a
4096-entry `lru_cache`, against 39.5 ms with no cache and 8.8 ms with no bound. A third payload there is worth
adding.
