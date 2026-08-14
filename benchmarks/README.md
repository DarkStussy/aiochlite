# Benchmarks

This directory contains benchmark scripts for `aiochlite`.

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
