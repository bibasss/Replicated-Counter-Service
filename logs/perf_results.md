# Task C4 performance results

Machine: macOS-15.7.7-arm64-arm-64bit, arm64, Python 3.9.6, 8 logical CPUs. Client and replicas run on the same machine (localhost).
Each configuration: 2000 measured Increments after a warm-up, repeated 3 times on fresh replicas. Values are the mean over repeats.

| Configuration | Median latency (ms) | p95 latency (ms) | Requests |
|---|---|---|---|
| Single replica, 1 client | 0.27 | 0.41 | 2000 |
| Single replica, 16 clients | 2.30 | 4.43 | 2000 |
| Quorum (3 replicas), 1 client | 0.45 | 0.64 | 2000 |
| Quorum (3 replicas), 16 clients | 5.68 | 14.60 | 2000 |

Variance and throughput (mean ± standard deviation over repeats):

| Configuration | Median (ms) | p95 (ms) | Throughput (req/s) |
|---|---|---|---|
| Single replica, 1 client | 0.27 ± 0.01 | 0.41 ± 0.07 | 3485 ± 277 |
| Single replica, 16 clients | 2.30 ± 0.11 | 4.43 ± 0.48 | 6358 ± 377 |
| Quorum (3 replicas), 1 client | 0.45 ± 0.01 | 0.64 ± 0.16 | 2124 ± 124 |
| Quorum (3 replicas), 16 clients | 5.68 ± 0.55 | 14.60 ± 1.47 | 2285 ± 216 |

Individual runs:

- Single replica, 1 client, run 1: median 0.26 ms, p95 0.42 ms, 3467 req/s, n=2000
- Single replica, 1 client, run 2: median 0.28 ms, p95 0.47 ms, 3218 req/s, n=2000
- Single replica, 1 client, run 3: median 0.26 ms, p95 0.33 ms, 3771 req/s, n=2000
- Single replica, 16 clients, run 1: median 2.42 ms, p95 4.97 ms, 5932 req/s, n=2000
- Single replica, 16 clients, run 2: median 2.28 ms, p95 4.21 ms, 6495 req/s, n=2000
- Single replica, 16 clients, run 3: median 2.21 ms, p95 4.09 ms, 6648 req/s, n=2000
- Quorum (3 replicas), 1 client, run 1: median 0.45 ms, p95 0.57 ms, 2163 req/s, n=2000
- Quorum (3 replicas), 1 client, run 2: median 0.46 ms, p95 0.82 ms, 1985 req/s, n=2000
- Quorum (3 replicas), 1 client, run 3: median 0.44 ms, p95 0.52 ms, 2225 req/s, n=2000
- Quorum (3 replicas), 16 clients, run 1: median 6.31 ms, p95 16.28 ms, 2038 req/s, n=2000
- Quorum (3 replicas), 16 clients, run 2: median 5.28 ms, p95 13.55 ms, 2434 req/s, n=2000
- Quorum (3 replicas), 16 clients, run 3: median 5.44 ms, p95 13.97 ms, 2382 req/s, n=2000
