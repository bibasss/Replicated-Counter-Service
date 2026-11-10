"""Lab 6-7 Task 4: PACELC benchmark, LINEARIZABLE vs FOLLOWER_STALE reads.

A 3-replica cluster on localhost (netsim.Cluster): A is the leaseholder and
sits with B in DC-1, C is a follower in DC-2. The benchmark client lives in
DC-2 and sends every Get to its local replica C:

  Scenario 1  LINEARIZABLE                       C proxies to the remote leaseholder A
  Scenario 2  FOLLOWER_STALE, max_staleness 5000  C answers from local state

Each scenario runs --iterations measured Gets (after a warm-up) in four
network configurations:

  localhost     no injected latency (the table the lab asks for)
  wan           every DC-1 <-> DC-2 link delayed by --wan-delay-ms one way
  partition     DC-1 | DC-2 cut for the whole run (Else -> Partition)
  sustained     partition held for --sustain-s, follower reads every 10 ms,
                to show where the 5000 ms staleness bound starts refusing

The first two are repeated --repeats times on fresh clusters; the tables
show the mean over repeats (and the standard deviation).

    python3 perf_cap_bench.py                      # ~2 minutes
    python3 perf_cap_bench.py --iterations 200 --repeats 1 --sustain-s 0   # quick
"""
import argparse
import json
import math
import os
import platform
import statistics
import sys
import time

import grpc

import kvstore_pb2
from client import KVClient, ReplicaUnavailableError, StaleReadExceededError
from netsim import Cluster

ROOT = os.path.dirname(os.path.abspath(__file__))
KEYS = [f"item-{i}" for i in range(20)]
STALENESS_MS = 5000
SCENARIOS = [
    ("Linearizable", "Leaseholder A (remote, via gateway C)", kvstore_pb2.LINEARIZABLE, 0),
    ("Follower Stale", "Local follower C", kvstore_pb2.FOLLOWER_STALE, STALENESS_MS),
]


def percentile(sorted_values, p):
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return float("nan")
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def run_reads(client, consistency, bound, iterations, warmup=100):
    """Returns (latencies_ms of successful reads, latencies_ms of failed reads, errors)."""
    for i in range(warmup):
        try:
            client.get(KEYS[i % len(KEYS)], consistency, bound)
        except (ReplicaUnavailableError, StaleReadExceededError, grpc.RpcError):
            pass
    ok, failed, errors = [], [], {}
    for i in range(iterations):
        start = time.perf_counter()
        try:
            client.get(KEYS[i % len(KEYS)], consistency, bound)
            ok.append((time.perf_counter() - start) * 1000)
        except (ReplicaUnavailableError, StaleReadExceededError, grpc.RpcError) as err:
            failed.append((time.perf_counter() - start) * 1000)
            name = type(err).__name__ if not isinstance(err, grpc.RpcError) else err.code().name
            errors[name] = errors.get(name, 0) + 1
    return ok, failed, errors


def summarize(ok, failed, errors, iterations):
    ok.sort()
    failed.sort()
    return dict(
        median_ms=statistics.median(ok) if ok else float("nan"),
        p95_ms=percentile(ok, 95),
        availability=100.0 * len(ok) / iterations,
        error_median_ms=statistics.median(failed) if failed else float("nan"),
        errors=errors, n=iterations)


def start_cluster(log_dir, wan_delay_ms=0):
    cluster = Cluster(log_dir=log_dir, wan_delay_ms=wan_delay_ms)
    with KVClient(cluster.target("A")) as a:
        for key in KEYS:
            a.put(key, f"value-of-{key}", kvstore_pb2.QUORUM_CP)
    # Let C's closed timestamp catch up past the preload writes.
    time.sleep(0.3)
    return cluster


def measure(config, log_dir, iterations, wan_delay_ms=0, partition=False):
    cluster = start_cluster(log_dir, wan_delay_ms)
    try:
        if partition:
            cluster.partition(["A", "B"], ["C"])
            time.sleep(0.2)
        results = {}
        with KVClient(cluster.target("C"), timeout=2) as client:
            for mode, _, consistency, bound in SCENARIOS:
                results[mode] = summarize(*run_reads(client, consistency, bound, iterations),
                                          iterations)
        return results
    finally:
        cluster.stop()


def sustained_partition(log_dir, seconds, interval_s=0.01):
    """Follower reads on C every 10 ms while DC-2 is cut off."""
    cluster = start_cluster(log_dir)
    timeline = []
    try:
        with KVClient(cluster.target("C"), timeout=2) as client:
            cluster.partition(["A", "B"], ["C"])
            cut_at = time.monotonic()
            i = 0
            while time.monotonic() - cut_at < seconds:
                t = time.monotonic() - cut_at
                try:
                    client.get(KEYS[i % len(KEYS)], kvstore_pb2.FOLLOWER_STALE, STALENESS_MS)
                    timeline.append((t, True))
                except StaleReadExceededError:
                    timeline.append((t, False))
                i += 1
                time.sleep(interval_s)
    finally:
        cluster.stop()
    first_refusal = next((t for t, ok in timeline if not ok), None)
    last_success = max((t for t, ok in timeline if ok), default=None)
    return dict(seconds=seconds, reads=len(timeline),
                availability=100.0 * sum(ok for _, ok in timeline) / len(timeline),
                last_success_s=last_success, first_refusal_s=first_refusal)


def mean_sd(values):
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return float("nan"), float("nan")
    return statistics.mean(values), statistics.stdev(values) if len(values) > 1 else 0.0


def fmt(x, digits=2):
    return "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{digits}f}"


def report(args, runs, partition, sustained):
    lines = [
        "# Lab 6-7 Task 4: PACELC benchmark results",
        "",
        f"Machine: {platform.platform()}, {platform.machine()}, Python "
        f"{platform.python_version()}, {os.cpu_count()} logical CPUs. All replicas and the "
        f"client run on localhost; replica-to-replica traffic goes through netsim.LinkProxy.",
        f"Client in DC-2, every Get sent to follower C. {args.iterations} measured Gets per "
        f"scenario after 100 warm-up Gets, {args.repeats} repeat(s) on fresh clusters; "
        f"FOLLOWER_STALE uses max_staleness_ms={STALENESS_MS}.",
        "",
    ]
    for config, title in (("localhost", "Localhost, no injected latency (lab table)"),
                          ("wan", f"Simulated WAN: DC-1 <-> DC-2 links +{args.wan_delay_ms} ms "
                                  f"one way ({2 * args.wan_delay_ms} ms RTT)")):
        lines += [f"## {title}", "",
                  "| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | "
                  "Read Availability (%) |",
                  "|---|---|---|---|---|"]
        for mode, target, *_ in SCENARIOS:
            med = mean_sd([r[config][mode]["median_ms"] for r in runs])
            p95 = mean_sd([r[config][mode]["p95_ms"] for r in runs])
            av = mean_sd([r[config][mode]["availability"] for r in runs])
            lines.append(f"| {mode} | {target} | {fmt(med[0])} ± {fmt(med[1])} | "
                         f"{fmt(p95[0])} ± {fmt(p95[1])} | {fmt(av[0], 1)} |")
        lines.append("")

    lines += ["## Partition DC-1 | DC-2 (client in DC-2, minority side)", "",
              "| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | "
              "Read Availability (%) | Median time to error (ms) | Errors |",
              "|---|---|---|---|---|---|---|"]
    for mode, target, *_ in SCENARIOS:
        r = partition[mode]
        errs = ", ".join(f"{k} x{v}" for k, v in r["errors"].items()) or "-"
        lines.append(f"| {mode} | {target} | {fmt(r['median_ms'])} | {fmt(r['p95_ms'])} | "
                     f"{fmt(r['availability'], 1)} | {fmt(r['error_median_ms'])} | {errs} |")
    lines.append("")
    if sustained:
        lines += [f"## Sustained partition: follower reads on C every 10 ms for "
                  f"{sustained['seconds']} s", "",
                  f"- reads: {sustained['reads']}, availability {fmt(sustained['availability'], 1)} %",
                  f"- last successful read at {fmt(sustained['last_success_s'])} s after the cut",
                  f"- first STALE_READ_EXCEEDED at {fmt(sustained['first_refusal_s'])} s after "
                  f"the cut (bound {STALENESS_MS / 1000:.0f} s)", ""]

    lines += ["## Individual runs", ""]
    for i, r in enumerate(runs, 1):
        for config in ("localhost", "wan"):
            for mode, *_ in SCENARIOS:
                x = r[config][mode]
                lines.append(f"- run {i}, {config}, {mode}: median {fmt(x['median_ms'])} ms, "
                             f"p95 {fmt(x['p95_ms'])} ms, availability "
                             f"{fmt(x['availability'], 1)} %, n={x['n']}")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--wan-delay-ms", type=int, default=10)
    parser.add_argument("--sustain-s", type=float, default=7.0,
                        help="length of the sustained-partition run (0 = skip)")
    parser.add_argument("--out", default=os.path.join(ROOT, "logs", "cap_bench_results.md"))
    args = parser.parse_args()
    log_root = os.path.join(ROOT, "logs", "cap_bench")

    runs = []
    for i in range(1, args.repeats + 1):
        run = {}
        for config, delay in (("localhost", 0), ("wan", args.wan_delay_ms)):
            print(f"run {i}/{args.repeats}: {config} ...", file=sys.stderr, flush=True)
            run[config] = measure(config, os.path.join(log_root, f"{config}-{i}"),
                                  args.iterations, wan_delay_ms=delay)
        runs.append(run)
    print("partition ...", file=sys.stderr, flush=True)
    partition = measure("partition", os.path.join(log_root, "partition"), args.iterations,
                        partition=True)
    sustained = None
    if args.sustain_s:
        print(f"sustained partition ({args.sustain_s:.0f} s) ...", file=sys.stderr, flush=True)
        sustained = sustained_partition(os.path.join(log_root, "sustained"), args.sustain_s)

    text = report(args, runs, partition, sustained)
    print(text)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        f.write(text)
    with open(os.path.splitext(args.out)[0] + ".json", "w") as f:
        json.dump(dict(args=vars(args), runs=runs, partition=partition, sustained=sustained),
                  f, indent=2)
    print(f"written to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
