"""Clients for the replica process.

CounterClient (Assignment 2): deadlines, bounded retries, idempotency keys,
quorum writes. Increment is sent to every replica with the same idempotency
key and is reported as committed only when a majority (2 of 3) acknowledged
it. Get reads from a single replica (the first one that answers).

KVClient (Lab 6-7): talks to one node of the key-value store (the "gateway"
node of the client's data centre) with a chosen read consistency and write
strategy. Failures surface as ReplicaUnavailableError / StaleReadExceededError.

QuorumClient (Lab 6-7): Dynamo-style client-side quorums over the replicas'
internal API, used to test R + W > N directly.

Usage:
    python3 client.py incr likes:post-42 --by 1 [--key KEY]
    python3 client.py get likes:post-42
    python3 client.py --targets localhost:50051 incr x      # single replica

    python3 client.py --targets localhost:50053 put user alice --strategy cp
    python3 client.py --targets localhost:50053 put user bob --strategy ap
    python3 client.py --targets localhost:50053 read user --consistency linearizable
    python3 client.py --targets localhost:50053 read user --consistency stale --max-staleness-ms 5000
"""
import argparse
import logging
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Optional

import grpc

import counter_pb2
import counter_pb2_grpc
import kvstore_pb2
import kvstore_pb2_grpc
from clocks import LamportClock

log = logging.getLogger("client")

DEFAULT_TARGETS = "localhost:50051,localhost:50052,localhost:50053"
TIMEOUT_S = 2.0
BACKOFF_S = (0.2, 0.4, 0.8)      # one pause per retry -> at most 3 retries
RETRYABLE = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


def describe(message):
    """Short text form of a request/reply for the event log."""
    if isinstance(message, counter_pb2.IncrementRequest):
        return f"Increment(counter={message.counter_id}, delta={message.delta})"
    if isinstance(message, counter_pb2.IncrementReply):
        return f"IncrementReply(new_value={message.new_value})"
    if isinstance(message, counter_pb2.GetRequest):
        return f"Get(counter={message.counter_id})"
    return f"GetReply(value={message.value})"


@dataclass
class IncrementResult:
    committed: bool                  # True when a majority acknowledged
    new_value: Optional[int]         # value agreed by the acks; None if not committed
    was_duplicate: bool              # every ack said "already applied"
    acks: int
    replicas: int
    errors: List[str] = field(default_factory=list)


class CounterClient:
    def __init__(self, targets=DEFAULT_TARGETS, name="client", timeout=TIMEOUT_S,
                 backoff=BACKOFF_S, log_path=None, echo=False):
        if isinstance(targets, str):
            targets = targets.split(",")
        self.name = name
        self._timeout = timeout
        self._backoff = tuple(backoff)
        self._channels = [grpc.insecure_channel(t) for t in targets]
        self._replicas = [(t, counter_pb2_grpc.CounterStub(ch))
                          for t, ch in zip(targets, self._channels)]
        self.majority = len(self._replicas) // 2 + 1
        self._clock = LamportClock(name, log_path, echo)
        self._pool = ThreadPoolExecutor(max_workers=2 * len(self._replicas))

    def _call(self, target, method, request, retry=True):
        """One RPC to one replica with a deadline; on a retryable error retry
        with exponential backoff. The same request (same idempotency key) is
        sent every time, only its Lamport timestamp is refreshed per attempt."""
        attempts = len(self._backoff) + 1 if retry else 1
        for attempt in range(1, attempts + 1):
            request.lamport_time = self._clock.send(f"{describe(request)} -> {target}")
            try:
                reply, call = method.with_call(
                    request, timeout=self._timeout, metadata=(("client-name", self.name),))
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt == attempts:
                    raise
                pause = self._backoff[attempt - 1]
                log.debug("attempt %d/%d to %s failed with %s, retrying in %.1fs",
                          attempt, attempts, target, err.code().name, pause)
                time.sleep(pause)
                continue
            replica = dict(call.trailing_metadata() or ()).get("replica-name", target)
            self._clock.receive(reply.lamport_time, f"{describe(reply)} <- {replica}")
            return reply

    def incr(self, counter_id, delta, key=None, wait_all=False):
        """Quorum write. Returns as soon as the outcome is known (a majority
        acked, or a majority can no longer be reached); with wait_all=True it
        waits for every replica, which gives an exact ack count."""
        # One key per logical operation: generated here, before the first
        # attempt, and reused by every retry and every replica.
        if key is None:
            key = str(uuid.uuid4())
        futures = [
            self._pool.submit(
                self._call, target, stub.Increment,
                counter_pb2.IncrementRequest(
                    counter_id=counter_id, delta=delta, idempotency_key=key))
            for target, stub in self._replicas
        ]
        total = len(futures)
        acks, errors = [], []
        for fut in as_completed(futures):
            try:
                acks.append(fut.result())
            except grpc.RpcError as err:
                errors.append(err.code().name)
            decided = len(acks) >= self.majority or len(errors) > total - self.majority
            if decided and not wait_all:
                break

        committed = len(acks) >= self.majority
        new_value = Counter(a.new_value for a in acks).most_common(1)[0][0] if committed else None
        return IncrementResult(
            committed=committed,
            new_value=new_value,
            was_duplicate=committed and all(a.was_duplicate for a in acks),
            acks=len(acks),
            replicas=total,
            errors=errors,
        )

    def get(self, counter_id):
        """Read from any single replica: try them in order, each round with
        one attempt per replica, and back off between rounds."""
        request = counter_pb2.GetRequest(counter_id=counter_id)
        last_error = None
        for pause in self._backoff + (None,):
            for target, stub in self._replicas:
                try:
                    return self._call(target, stub.Get, request, retry=False)
                except grpc.RpcError as err:
                    if err.code() not in RETRYABLE:
                        raise
                    last_error = err
            if pause is not None:
                time.sleep(pause)
        raise last_error

    def close(self):
        # Wait for calls still in flight to the slowest replica.
        self._pool.shutdown(wait=True)
        for channel in self._channels:
            channel.close()
        self._clock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---------------------------------------------------------------------------
# Key-value store (Lab 6-7)
# ---------------------------------------------------------------------------

class ReplicaUnavailableError(Exception):
    """The request needs the leaseholder or a majority and cannot get it
    (CP write on a minority partition, linearizable read without a lease)."""

    def __init__(self, message, reply=None):
        super().__init__(message)
        self.reply = reply            # PutReply for a rejected write, else None


class StaleReadExceededError(Exception):
    """A follower read was refused: the follower lags more than max_staleness_ms."""


STRATEGIES = {"cp": kvstore_pb2.QUORUM_CP, "ap": kvstore_pb2.ALWAYS_AP}
CONSISTENCIES = {"linearizable": kvstore_pb2.LINEARIZABLE, "stale": kvstore_pb2.FOLLOWER_STALE}


def _as_kv_error(err):
    """Map a gRPC error from the KV service to the lab's error types."""
    details = err.details() or ""
    if err.code() == grpc.StatusCode.FAILED_PRECONDITION and details.startswith("STALE_READ_EXCEEDED"):
        return StaleReadExceededError(details)
    if err.code() in RETRYABLE:
        return ReplicaUnavailableError(details or err.code().name)
    return None


class KVClient:
    """Client of one key-value node (the gateway of the client's data centre)."""

    def __init__(self, target, timeout=TIMEOUT_S, backoff=BACKOFF_S):
        self.target = target
        self._timeout = timeout
        self._backoff = tuple(backoff)
        self._channel = grpc.insecure_channel(target)
        self._stub = kvstore_pb2_grpc.KeyValueStoreStub(self._channel)

    def put(self, key, value, strategy=kvstore_pb2.QUORUM_CP, timestamp=0,
            idempotency_key=None, check=True):
        """Put with one idempotency key for every retry. Transport errors are
        retried with backoff; a rejected write (success=false) is not.
        check=True raises ReplicaUnavailableError on rejection, otherwise the
        PutReply is returned as is."""
        if isinstance(strategy, str):
            strategy = STRATEGIES[strategy]
        request = kvstore_pb2.PutRequest(
            key=key, value=value, strategy=strategy, timestamp=timestamp,
            idempotency_key=idempotency_key or str(uuid.uuid4()))
        for pause in self._backoff + (None,):
            try:
                reply = self._stub.Put(request, timeout=self._timeout)
                break
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or pause is None:
                    raise _as_kv_error(err) or err
                time.sleep(pause)
        if check and not reply.success:
            raise ReplicaUnavailableError(reply.error_message, reply)
        return reply

    def get(self, key, consistency=kvstore_pb2.LINEARIZABLE, max_staleness_ms=0):
        """One attempt, no retries, so callers can measure availability."""
        if isinstance(consistency, str):
            consistency = CONSISTENCIES[consistency]
        request = kvstore_pb2.GetRequest(key=key, consistency=consistency,
                                         max_staleness_ms=max_staleness_ms)
        try:
            return self._stub.Get(request, timeout=self._timeout)
        except grpc.RpcError as err:
            raise _as_kv_error(err) or err

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


@dataclass
class QuorumRead:
    value: Optional[str]              # value of the newest version among the R replies
    timestamp: int
    replies: int


class QuorumClient:
    """Client-side quorums (Dynamo style) over the replicas' internal API:
    a write goes to W chosen replicas, a read asks R chosen replicas and keeps
    the version with the largest (timestamp, origin). Run against replicas with
    anti-entropy off, so a value is only where the client put it."""

    def __init__(self, targets, timeout=TIMEOUT_S):
        if isinstance(targets, str):
            targets = targets.split(",")
        self.targets = list(targets)
        self.n = len(self.targets)
        self._timeout = timeout
        self._channels = [grpc.insecure_channel(t) for t in self.targets]
        self._stubs = [kvstore_pb2_grpc.ReplicaPeerStub(ch) for ch in self._channels]

    def write(self, key, value, replicas, timestamp=None, origin="quorum-client"):
        """Write one version to the replicas with the given indexes. Returns acks."""
        entry = kvstore_pb2.Entry(key=key, value=value, origin=origin,
                                  timestamp=timestamp or time.time_ns() // 1000)
        calls = [self._stubs[i].Replicate.future(entry, timeout=self._timeout) for i in replicas]
        acks = 0
        for call in calls:
            try:
                call.result()
                acks += 1
            except grpc.RpcError:
                pass
        return acks

    def read(self, key, replicas):
        calls = [self._stubs[i].ReadLocal.future(kvstore_pb2.ReadLocalRequest(key=key),
                                                 timeout=self._timeout) for i in replicas]
        newest, replies = None, 0
        for call in calls:
            try:
                reply = call.result()
            except grpc.RpcError:
                continue
            replies += 1
            if reply.found and (newest is None or
                                (reply.entry.timestamp, reply.entry.origin) >
                                (newest.timestamp, newest.origin)):
                newest = reply.entry
        return QuorumRead(newest.value if newest else None,
                          newest.timestamp if newest else 0, replies)

    def close(self):
        for channel in self._channels:
            channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--targets", default=DEFAULT_TARGETS,
                        help="comma-separated host:port list of replicas")
    parser.add_argument("--name", default="client-1", help="client name used in event logs")
    parser.add_argument("--trace", action="store_true", help="print Lamport event lines to stderr")
    sub = parser.add_subparsers(dest="command", required=True)

    p_incr = sub.add_parser("incr", help="increment a counter")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1, help="delta to add")
    p_incr.add_argument("--key", help="idempotency key (default: a new uuid4)")

    p_get = sub.add_parser("get", help="read a counter")
    p_get.add_argument("counter_id")

    p_put = sub.add_parser("put", help="key-value store: write KEY=VALUE via the first target")
    p_put.add_argument("key")
    p_put.add_argument("value")
    p_put.add_argument("--strategy", choices=STRATEGIES, default="cp",
                       help="cp = QUORUM_CP (majority), ap = ALWAYS_AP (any node)")
    p_put.add_argument("--timestamp", type=int, default=0,
                       help="LWW version in microseconds (default: assigned by the server)")
    p_put.add_argument("--key", dest="idem", help="idempotency key (default: a new uuid4)")

    p_read = sub.add_parser("read", help="key-value store: read KEY via the first target")
    p_read.add_argument("key")
    p_read.add_argument("--consistency", choices=CONSISTENCIES, default="linearizable")
    p_read.add_argument("--max-staleness-ms", type=int, default=5000,
                        help="bound for --consistency stale (0 = no bound)")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    if args.command in ("put", "read"):
        sys.exit(kv_command(args))

    with CounterClient(args.targets, args.name, echo=args.trace) as client:
        try:
            if args.command == "incr":
                r = client.incr(args.counter_id, args.by, args.key, wait_all=True)
                acked = f"replicas acked: {r.acks}/{r.replicas}"
                if r.committed:
                    duplicate = "yes" if r.was_duplicate else "no"
                    print(f"OK committed value={r.new_value} ({acked}, duplicate: {duplicate})")
                else:
                    print(f"FAILED not committed ({acked}, need {client.majority}; "
                          f"errors: {', '.join(r.errors)})", file=sys.stderr)
                    sys.exit(1)
            else:
                reply = client.get(args.counter_id)
                print(f"value={reply.value}" if reply.found else "not found")
        except grpc.RpcError as err:
            print(f"FAILED {err.code().name}: {err.details()}", file=sys.stderr)
            sys.exit(1)


def kv_command(args):
    target = args.targets.split(",")[0]
    with KVClient(target) as client:
        try:
            if args.command == "put":
                r = client.put(args.key, args.value, args.strategy, args.timestamp, args.idem)
                print(f"OK success=true acks_received={r.acks_received}")
            else:
                r = client.get(args.key, args.consistency, args.max_staleness_ms)
                shown = f"value={r.value}" if r.found else "not found"
                print(f"{shown} (read_timestamp={r.read_timestamp}, is_stale={str(r.is_stale).lower()})")
        except (ReplicaUnavailableError, StaleReadExceededError) as err:
            print(f"FAILED {type(err).__name__}: {err}", file=sys.stderr)
            return 1
        except grpc.RpcError as err:
            print(f"FAILED {err.code().name}: {err.details()}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    main()
