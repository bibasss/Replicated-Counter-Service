# Task C4 performance results

Machine: macOS-15.7.7-arm64-arm-64bit, arm64, Python 3.9.6, 8 logical CPUs. Client and replicas run on the same machine (localhost).
Each configuration: 2000 measured Increments after a warm-up, repeated 3 times on fresh replicas. Values are the mean over repeats.

| Configuration | Median latency (ms) | p95 latency (ms) | Requests |
|---|---|---|---|
| Single replica, 1 client | 0.26 | 0.31 | 2000 |
| Single replica, 16 clients | 2.23 | 4.42 | 2000 |
| Quorum (3 replicas), 1 client | 0.44 | 0.59 | 2000 |
| Quorum (3 replicas), 16 clients | 5.50 | 14.70 | 2000 |

Variance and throughput (mean ± standard deviation over repeats):

| Configuration | Median (ms) | p95 (ms) | Throughput (req/s) |
|---|---|---|---|
| Single replica, 1 client | 0.26 ± 0.00 | 0.31 ± 0.01 | 3763 ± 8 |
| Single replica, 16 clients | 2.23 ± 0.12 | 4.42 ± 0.85 | 6465 ± 702 |
| Quorum (3 replicas), 1 client | 0.44 ± 0.01 | 0.59 ± 0.16 | 2193 ± 166 |
| Quorum (3 replicas), 16 clients | 5.50 ± 0.58 | 14.70 ± 2.59 | 2343 ± 289 |

Individual runs:

- Single replica, 1 client, run 1: median 0.26 ms, p95 0.33 ms, 3756 req/s, n=2000
- Single replica, 1 client, run 2: median 0.26 ms, p95 0.30 ms, 3763 req/s, n=2000
- Single replica, 1 client, run 3: median 0.26 ms, p95 0.31 ms, 3771 req/s, n=2000
- Single replica, 16 clients, run 1: median 2.17 ms, p95 3.91 ms, 6845 req/s, n=2000
- Single replica, 16 clients, run 2: median 2.38 ms, p95 5.40 ms, 5654 req/s, n=2000
- Single replica, 16 clients, run 3: median 2.15 ms, p95 3.93 ms, 6895 req/s, n=2000
- Quorum (3 replicas), 1 client, run 1: median 0.43 ms, p95 0.50 ms, 2281 req/s, n=2000
- Quorum (3 replicas), 1 client, run 2: median 0.43 ms, p95 0.50 ms, 2296 req/s, n=2000
- Quorum (3 replicas), 1 client, run 3: median 0.45 ms, p95 0.78 ms, 2002 req/s, n=2000
- Quorum (3 replicas), 16 clients, run 1: median 5.17 ms, p95 12.88 ms, 2514 req/s, n=2000
- Quorum (3 replicas), 16 clients, run 2: median 5.15 ms, p95 13.54 ms, 2506 req/s, n=2000
- Quorum (3 replicas), 16 clients, run 3: median 6.17 ms, p95 17.66 ms, 2009 req/s, n=2000
