"""Task C4: Increment latency, single replica vs three-replica quorum.

For each configuration (1 or 3 replicas) x (1 or 16 concurrent clients) it
sends REQUESTS Increments (split evenly between the clients), records the
latency of every call, and reports median and p95. The whole measurement is
repeated REPEATS times on fresh replicas to show run-to-run variance.

p95 is the nearest-rank percentile: sort the latencies and take the value at
1-based index ceil(0.95 * n) (0-based index ceil(0.95 * n) - 1).

Run from the repository root:
    python3 tests/perf_benchmark.py [--requests 2000] [--repeats 3]
Results are printed and written to logs/perf_results.md.
"""
import argparse
import math
import os
import platform
import statistics
import subprocess
import sys
import threading
import time

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from client import CounterClient  # noqa: E402

BASE_PORT = 50151
WARMUP = 50


def p95(sorted_values):
    return sorted_values[math.ceil(0.95 * len(sorted_values)) - 1]


def start_replicas(n):
    procs, targets = [], []
    for i in range(n):
        port = BASE_PORT + i
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "server.py"), "--port", str(port), "--quiet"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        targets.append(f"localhost:{port}")
    for target in targets:
        with grpc.insecure_channel(target) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
    return procs, targets


def measure(targets, clients, requests):
    """Returns (sorted latencies in ms, wall-clock seconds)."""
    per_client = requests // clients
    latencies = [[] for _ in range(clients)]
    failures = []
    barrier = threading.Barrier(clients + 1)

    def worker(i):
        with CounterClient(targets, name=f"bench-{i}") as client:
            for _ in range(WARMUP // clients + 1):          # warm up the channel
                client.incr(f"warm-{i}", 1)
            barrier.wait()
            for _ in range(per_client):
                start = time.perf_counter()
                result = client.incr(f"bench-{i}", 1)
                latencies[i].append((time.perf_counter() - start) * 1000)
                if not result.committed:
                    failures.append(result)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()
    barrier.wait()
    wall_start = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - wall_start
    if failures:
        raise RuntimeError(f"{len(failures)} writes did not commit")
    return sorted(x for per in latencies for x in per), wall


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    configs = [
        ("Single replica, 1 client", 1, 1),
        ("Single replica, 16 clients", 1, 16),
        ("Quorum (3 replicas), 1 client", 3, 1),
        ("Quorum (3 replicas), 16 clients", 3, 16),
    ]
    runs = {name: [] for name, _, _ in configs}
    for repeat in range(1, args.repeats + 1):
        for name, replicas, clients in configs:
            procs, targets = start_replicas(replicas)
            try:
                lat, wall = measure(targets, clients, args.requests)
            finally:
                for p in procs:
                    p.terminate()
                    p.wait()
            row = dict(median=statistics.median(lat), p95=p95(lat), n=len(lat),
                       throughput=len(lat) / wall)
            runs[name].append(row)
            print(f"run {repeat}: {name:<34} median={row['median']:.2f} ms  "
                  f"p95={row['p95']:.2f} ms  n={row['n']}  {row['throughput']:.0f} req/s",
                  flush=True)

    def mean_sd(name, key):
        values = [r[key] for r in runs[name]]
        sd = statistics.stdev(values) if len(values) > 1 else 0.0
        return statistics.mean(values), sd

    out = [
        "# Task C4 performance results",
        "",
        f"Machine: {platform.platform()}, {platform.machine()}, Python {platform.python_version()}, "
        f"{os.cpu_count()} logical CPUs. Client and replicas run on the same machine (localhost).",
        f"Each configuration: {args.requests} measured Increments after a warm-up, "
        f"repeated {args.repeats} times on fresh replicas. Values are the mean over repeats.",
        "",
        "| Configuration | Median latency (ms) | p95 latency (ms) | Requests |",
        "|---|---|---|---|",
    ]
    for name, _, _ in configs:
        med, _ = mean_sd(name, "median")
        p, _ = mean_sd(name, "p95")
        out.append(f"| {name} | {med:.2f} | {p:.2f} | {runs[name][0]['n']} |")
    out += [
        "",
        "Variance and throughput (mean ± standard deviation over repeats):",
        "",
        "| Configuration | Median (ms) | p95 (ms) | Throughput (req/s) |",
        "|---|---|---|---|",
    ]
    for name, _, _ in configs:
        cells = []
        for key, fmt in (("median", ".2f"), ("p95", ".2f"), ("throughput", ".0f")):
            m, sd = mean_sd(name, key)
            cells.append(f"{m:{fmt}} ± {sd:{fmt}}")
        out.append(f"| {name} | " + " | ".join(cells) + " |")
    out += ["", "Individual runs:", ""]
    for name, _, _ in configs:
        for i, r in enumerate(runs[name], 1):
            out.append(f"- {name}, run {i}: median {r['median']:.2f} ms, p95 {r['p95']:.2f} ms, "
                       f"{r['throughput']:.0f} req/s, n={r['n']}")

    text = "\n".join(out) + "\n"
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    with open(os.path.join(ROOT, "logs", "perf_results.md"), "w") as f:
        f.write(text)
    print()
    print(text)


if __name__ == "__main__":
    main()
