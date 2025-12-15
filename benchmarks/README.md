# Benchmarks

This directory contains benchmark scripts for `aiochlite`.

> [!NOTE]
> Benchmarks always depend on machine and environment (CPU, RAM, kernel, ClickHouse version/config, network, etc).
> The sample output in this document was captured on a local machine with 6 CPU cores and 32 GB RAM, running ClickHouse 25.8 LTS.

## IO benchmark: fetch + decode

Script: `benchmarks/fetch_rows.py`

What it measures:
- End-to-end `SELECT` fetch + decode time.
- Data is generated on the server (`INSERT ... SELECT ... FROM numbers(...)`) to avoid measuring client-side inserts.
- Compares:
  - `aiochlite (Row)`: `AsyncChClient.fetch()` (returns `Row`)
  - `aiochlite (tuples)`: `AsyncChClient.fetch_rows()` (returns raw tuples)
  - `clickhouse-connect (async)`
  - `aiochclient`

Run:

```bash
python benchmarks/fetch_rows.py
```

Tune (optional):

```bash
BENCH_ROWS=100000 BENCH_ROUNDS=5 BENCH_WARMUP=2 python benchmarks/fetch_rows.py
```

Environment variables:
- `CLICKHOUSE_HOST` (default: `localhost`)
- `CLICKHOUSE_PORT` (default: `8123`)
- `CLICKHOUSE_USER` (default: `default`)
- `CLICKHOUSE_PASSWORD` (default: empty)
- `CLICKHOUSE_DATABASE` (default: `default`)
- `BENCH_ROWS` (default: `10000`)
- `BENCH_ROUNDS` (default: `3`)
- `BENCH_WARMUP` (default: `1`)

### Sample output

```
Rows: 100000, rounds: 5, warmup: 2
Table: bench_io_4a244539576846b78b4f636545b578ac

IO benchmark (clickhouse-connect (async))
Round 1:   437.98 ms (228,323 rows/s, 4.4 µs/row)
Round 2:   432.25 ms (231,349 rows/s, 4.3 µs/row)
Round 3:   423.91 ms (235,898 rows/s, 4.2 µs/row)
Round 4:   433.05 ms (230,918 rows/s, 4.3 µs/row)
Round 5:   439.55 ms (227,505 rows/s, 4.4 µs/row)
Avg:        433.35 ms (230,761 rows/s, 4.3 µs/row)

IO benchmark (aiochlite (Row))
Round 1:   516.16 ms (193,740 rows/s, 5.2 µs/row)
Round 2:   515.42 ms (194,016 rows/s, 5.2 µs/row)
Round 3:   521.92 ms (191,600 rows/s, 5.2 µs/row)
Round 4:   521.53 ms (191,744 rows/s, 5.2 µs/row)
Round 5:   531.39 ms (188,186 rows/s, 5.3 µs/row)
Avg:        521.28 ms (191,834 rows/s, 5.2 µs/row)

IO benchmark (aiochlite (tuples))
Round 1:   457.12 ms (218,761 rows/s, 4.6 µs/row)
Round 2:   464.92 ms (215,091 rows/s, 4.6 µs/row)
Round 3:   457.96 ms (218,357 rows/s, 4.6 µs/row)
Round 4:   471.56 ms (212,062 rows/s, 4.7 µs/row)
Round 5:   454.70 ms (219,927 rows/s, 4.5 µs/row)
Avg:        461.25 ms (216,801 rows/s, 4.6 µs/row)

IO benchmark (aiochclient)
Round 1:  1560.18 ms (64,095 rows/s, 15.6 µs/row)
Round 2:  1550.37 ms (64,501 rows/s, 15.5 µs/row)
Round 3:  1557.22 ms (64,217 rows/s, 15.6 µs/row)
Round 4:  1579.76 ms (63,301 rows/s, 15.8 µs/row)
Round 5:  1546.31 ms (64,670 rows/s, 15.5 µs/row)
Avg:       1558.77 ms (64,153 rows/s, 15.6 µs/row)
```
