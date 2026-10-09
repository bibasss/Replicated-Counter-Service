"""Counter client: deadlines, bounded retries, idempotency keys, quorum writes.

Increment is sent to every replica with the same idempotency key and is
reported as committed only when a majority (2 of 3) acknowledged it.
Get reads from a single replica (the first one that answers).

Usage:
    python3 client.py incr likes:post-42 --by 1 [--key KEY]
    python3 client.py get likes:post-42
    python3 client.py --targets localhost:50051 incr x      # single replica
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

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

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


if __name__ == "__main__":
    main()
