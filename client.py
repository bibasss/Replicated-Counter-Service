"""Counter client with deadlines, bounded retries and idempotency keys.

Usage:
    python3 client.py incr likes:post-42 --by 1 [--key KEY]
    python3 client.py get likes:post-42
"""
import argparse
import logging
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

log = logging.getLogger("client")

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


class CounterClient:
    def __init__(self, target="localhost:50051", name="client", timeout=TIMEOUT_S,
                 backoff=BACKOFF_S, log_path=None, echo=False):
        self.name = name
        self._target = target
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._timeout = timeout
        self._backoff = backoff
        self._clock = LamportClock(name, log_path, echo)

    def _call(self, method, request):
        """One RPC with a deadline; on a retryable error retry with exponential
        backoff. The same request (same idempotency key) is sent every time,
        only its Lamport timestamp is refreshed per attempt."""
        attempts = len(self._backoff) + 1
        for attempt in range(1, attempts + 1):
            request.lamport_time = self._clock.send(f"{describe(request)} -> {self._target}")
            try:
                reply, call = method.with_call(
                    request, timeout=self._timeout, metadata=(("client-name", self.name),))
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt == attempts:
                    raise
                pause = self._backoff[attempt - 1]
                log.warning("attempt %d/%d to %s failed with %s, retrying in %.1fs",
                            attempt, attempts, self._target, err.code().name, pause)
                time.sleep(pause)
                continue
            replica = dict(call.trailing_metadata() or ()).get("replica-name", self._target)
            self._clock.receive(reply.lamport_time, f"{describe(reply)} <- {replica}")
            return reply

    def incr(self, counter_id, delta, key=None):
        # One key per logical operation: generated here, before the first
        # attempt, and reused by every retry inside _call.
        if key is None:
            key = str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key)
        return self._call(self._stub.Increment, request)

    def get(self, counter_id):
        return self._call(self._stub.Get, counter_pb2.GetRequest(counter_id=counter_id))

    def close(self):
        self._channel.close()
        self._clock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--target", default="localhost:50051", help="host:port of the replica")
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

    with CounterClient(args.target, args.name, echo=args.trace) as client:
        try:
            if args.command == "incr":
                reply = client.incr(args.counter_id, args.by, args.key)
                duplicate = "yes" if reply.was_duplicate else "no"
                print(f"OK value={reply.new_value} (duplicate: {duplicate})")
            else:
                reply = client.get(args.counter_id)
                print(f"value={reply.value}" if reply.found else "not found")
        except grpc.RpcError as err:
            print(f"FAILED {err.code().name}: {err.details()}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
