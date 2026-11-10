# Replicated Counter Service

A gRPC counter service in three stages: a single replica with retry-safe
increments (Part A), Lamport clock instrumentation (Part B), and three
replicas with majority-acknowledged writes plus a test suite (Part C).

All commands are run from the repository root.

## Setup

```bash
python3 --version                      # 3.9 or later
python3 -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install grpcio grpcio-tools pytest
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

## Start one replica

```bash
python3 server.py --port 50051 --name replica-A
```

In another terminal (`--targets` selects the replica list; with one replica
the majority is 1):

```bash
python3 client.py --targets localhost:50051 incr likes:post-42 --by 5
python3 client.py --targets localhost:50051 get likes:post-42          # value=5
```

## Start three replicas

```bash
python3 server.py --port 50051 --name replica-A &
python3 server.py --port 50052 --name replica-B &
python3 server.py --port 50053 --name replica-C &
```

The client talks to `localhost:50051,50052,50053` by default:

```bash
python3 client.py incr likes:post-42 --by 1
python3 client.py incr likes:post-42 --by 1 --key 9f3c-demo
python3 client.py incr likes:post-42 --by 1 --key 9f3c-demo   # same key: duplicate: yes
python3 client.py get likes:post-42
python3 client.py --trace get likes:post-42                    # also print Lamport events
kill %1 %2 %3                                                  # stop the replicas
```

Replica options:

| Flag | Meaning |
|---|---|
| `--port` | listen port |
| `--name` | name in event logs (default `replica-<port>`) |
| `--log-file PATH` | append Lamport event lines to a file |
| `--quiet` | do not print event lines |
| `--delay-ms N` | delay Increment replies by N ms (the write is already applied) |
| `--fault slow-first` | with `--delay-ms`: delay only the first reply for each key |
| `--fault crash-before-apply` | die on the first Increment before applying it |
| `--fault crash-after-apply` | die on the first Increment after applying it, before replying |

## Run the test suite

```bash
python3 -m pytest -v                      # all tests (about 1 minute)
python3 -m pytest tests/test_counter.py   # (about 2 s)
python3 -m pytest tests/test_failures.py  # real replica processes
```

Failure tests keep each replica's log in `logs/test_failures/<test name>/`.

## Run the benchmark 

```bash
python3 tests/perf_benchmark.py           # 4 configurations x 2000 requests x 3 repeats
```

Results are printed and written to `logs/perf_results.md`.

## Lamport trace scenario 

```bash
python3 tests/lamport_scenario.py
```

Starts replicas A/B/C on ports 50051-50053 and runs client-1 (2 x incr x),
client-2 (2 x incr y) and client-3 (get only) concurrently. Per-process logs,
the merged trace and the replica-A excerpt are written to `logs/b2/`.

## Layout

```
counter.proto                 service definition
counter_pb2*.py               generated stubs (do not edit)
server.py                     replica
client.py                     client: retries, idempotency keys, quorum writes
clocks.py                     Lamport clock
tests/test_counter.py         Tests
tests/test_failures.py        Failure-injection tests
tests/perf_benchmark.py       Benchmark
tests/lamport_scenario.py     Scenario
logs/                         captured traces, test and benchmark output
report.pdf                    test report
```
