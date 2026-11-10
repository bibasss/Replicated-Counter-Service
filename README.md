# Replicated Counter Service

A gRPC counter service in three stages: a single replica with retry-safe
increments (Part A), Lamport clock instrumentation (Part B), and three
replicas with majority-acknowledged writes plus a test suite (Part C).

Lab 6-7 (Consistency Models & the CAP Theorem) adds a replicated key-value
store to the same replica process: configurable read consistency
(LINEARIZABLE / FOLLOWER_STALE), write strategies (QUORUM_CP / ALWAYS_AP),
LWW conflict resolution, partition tests and a PACELC benchmark. See
[Lab 6-7: key-value store](#lab-6-7-key-value-store-cap--pacelc) below.

All commands are run from the repository root.

## Setup

```bash
python3 --version                      # 3.9 or later
python3 -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r requirements.txt            # grpcio, grpcio-tools, protobuf, pytest
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto kvstore.proto
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

## Lab 6-7: key-value store (CAP / PACELC)

### Design

```
                 client (DC-1)                         client (DC-2)
                      |                                     |
       +--------------v-----+    +----------------+    +----v-----------+
       | A :50051           |<-->| B :50052       |    | C :50053       |
       | leaseholder [a-z]  |    | follower       |<-->| follower       |
       +--------------------+    +----------------+    +----------------+
       |<----------- DC-1 ------------------------>|   |<--- DC-2 ----->|
```

| Request | Behaviour |
|---|---|
| `Get LINEARIZABLE` | served only by the leaseholder A; a follower proxies it. A refuses it when its lease expired (no heartbeat ack from a majority within `--lease-ms`). |
| `Get FOLLOWER_STALE` | served from the local replica. A follower has every committed CP write up to the *closed timestamp* carried by A's heartbeats; if `now - closed_ts > max_staleness_ms` it fails with `FAILED_PRECONDITION` `STALE_READ_EXCEEDED`. `max_staleness_ms = 0` means no bound. |
| `Put QUORUM_CP` | forwarded to A, which replicates to all peers and commits with a majority (2 of 3). Otherwise nothing is applied: `success=false`, `ReplicaUnavailableError: quorum lost`. |
| `Put ALWAYS_AP` | applied at once on the receiving node and pushed to the peers it can reach; `acks_received` = replicas that hold it. |

Every value has a version `(timestamp µs, origin node)`; replicas merge by
Last-Write-Wins. Anti-entropy (`--anti-entropy-ms`, full state exchange)
makes divergent AP writes converge after a partition heals; CP writes a
follower missed are re-sent with the next heartbeat.

### Start a three-node cluster

```bash
python3 server.py --port 50051 --name A --leaseholder A --peers B=localhost:50052,C=localhost:50053 --quiet &
python3 server.py --port 50052 --name B --leaseholder A --peers A=localhost:50051,C=localhost:50053 --quiet &
python3 server.py --port 50053 --name C --leaseholder A --peers A=localhost:50051,B=localhost:50052 --quiet &

python3 client.py --targets localhost:50053 put user alice --strategy cp       # OK acks_received=3
python3 client.py --targets localhost:50053 read user                          # linearizable, via A
python3 client.py --targets localhost:50053 read user --consistency stale --max-staleness-ms 5000
python3 client.py --targets localhost:50053 put user bob --strategy ap
kill %1 %2 %3
```

To try partitions by hand, `python3 netsim.py` starts the same cluster with
every replica-to-replica link behind a TCP proxy and reads commands from
stdin: `partition A,B C`, `heal`, `kill C`, `quit`.

| Replica flag (KV) | Default | Meaning |
|---|---|---|
| `--peers NAME=HOST:PORT,...` | none | other nodes (none = single-node cluster) |
| `--leaseholder NAME` | this node | holder of the lease for range `[a-z]` |
| `--heartbeat-ms` | 100 | heartbeat / closed timestamp interval |
| `--lease-ms` | 1000 | lease lifetime without majority heartbeat acks |
| `--anti-entropy-ms` | 500 | LWW state sync interval (0 = off) |
| `--peer-timeout-ms` | 500 | deadline for replica-to-replica calls |

### Tests and benchmark

```bash
python3 -m pytest tests/test_cap.py -v     # 17 tests, about 25 s
python3 perf_cap_bench.py                  # about 2 minutes -> logs/cap_bench_results.md
```

`tests/test_cap.py` partitions the cluster into DC-1 (A, B) | DC-2 (C) by
cutting proxies (`netsim.py`). Replica logs are kept in
`logs/test_cap/<test name>/`, the R/W/N matrix in `logs/test_cap/quorum_matrix.md`.

## Layout

```
counter.proto                 service definition (counter)
kvstore.proto                 service definition (key-value store, Lab 6-7)
*_pb2*.py                     generated stubs (do not edit)
server.py                     replica: Counter + KeyValueStore + ReplicaPeer
client.py                     CounterClient, KVClient, QuorumClient, CLI
clocks.py                     Lamport clock
netsim.py                     replica processes behind TCP proxies (partitions, WAN delay)
perf_cap_bench.py             Lab 6-7 PACELC benchmark
tests/test_counter.py         Tests
tests/test_failures.py        Failure-injection tests
tests/test_cap.py             Lab 6-7 quorum / partition tests
tests/perf_benchmark.py       Benchmark
tests/lamport_scenario.py     Scenario
logs/                         captured traces, test and benchmark output
report.pdf                    test report (Assignment 2)
Lab_Report_CAP.pdf            lab report (Lab 6-7)
```
