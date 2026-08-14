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
  - `aiochlite (Row, lazy_decode=False)`: `AsyncChClient.fetch()` (returns `Row`). Because the benchmark reads
    every column, eager decoding provides the fairest comparison. The client defaults to `lazy_decode=True`.
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
Clients: aiochlite 1.3.0, aiochclient 2.7.0, clickhouse-connect 1.7.1
Python: 3.14.5, ClickHouse: 26.3.17.110
Rows: 100000, rounds: 5, warmup: 2
Table: bench_io_27d4bb60762f49d5a463ff821caa203c

IO benchmark (clickhouse-connect (async))
Round 1:   161.16 ms (620,506 rows/s, 1.6 µs/row)
Round 2:   159.96 ms (625,158 rows/s, 1.6 µs/row)
Round 3:   154.63 ms (646,686 rows/s, 1.5 µs/row)
Round 4:   153.14 ms (653,014 rows/s, 1.5 µs/row)
Round 5:   153.95 ms (649,576 rows/s, 1.5 µs/row)
Avg:       156.57 ms (638,704 rows/s, 1.6 µs/row)

IO benchmark (aiochlite (Row, lazy_decode=False))
Round 1:   333.75 ms (299,624 rows/s, 3.3 µs/row)
Round 2:   346.22 ms (288,836 rows/s, 3.5 µs/row)
Round 3:   333.19 ms (300,132 rows/s, 3.3 µs/row)
Round 4:   331.88 ms (301,312 rows/s, 3.3 µs/row)
Round 5:   323.14 ms (309,467 rows/s, 3.2 µs/row)
Avg:       333.63 ms (299,729 rows/s, 3.3 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   288.21 ms (346,972 rows/s, 2.9 µs/row)
Round 2:   286.60 ms (348,920 rows/s, 2.9 µs/row)
Round 3:   288.83 ms (346,220 rows/s, 2.9 µs/row)
Round 4:   286.15 ms (349,471 rows/s, 2.9 µs/row)
Round 5:   288.96 ms (346,064 rows/s, 2.9 µs/row)
Avg:       287.75 ms (347,524 rows/s, 2.9 µs/row)

IO benchmark (aiochclient)
Round 1:   344.93 ms (289,916 rows/s, 3.4 µs/row)
Round 2:   348.51 ms (286,935 rows/s, 3.5 µs/row)
Round 3:   358.83 ms (278,680 rows/s, 3.6 µs/row)
Round 4:   350.65 ms (285,184 rows/s, 3.5 µs/row)
Round 5:   348.19 ms (287,196 rows/s, 3.5 µs/row)
Avg:       350.22 ms (285,532 rows/s, 3.5 µs/row)
```

A second run with the same configuration produced averages within approximately 2% of these results.
