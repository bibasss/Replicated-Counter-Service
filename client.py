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

log = logging.getLogger("client")

TIMEOUT_S = 2.0
BACKOFF_S = (0.2, 0.4, 0.8)      # one pause per retry -> at most 3 retries
RETRYABLE = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


def call_with_retry(rpc, request, timeout=TIMEOUT_S, backoff=BACKOFF_S):
    """Call rpc(request); on a retryable error retry with exponential backoff.

    The same request object (and therefore the same idempotency key) is sent
    on every attempt.
    """
    attempts = len(backoff) + 1
    for attempt in range(1, attempts + 1):
        try:
            return rpc(request, timeout=timeout)
        except grpc.RpcError as err:
            if err.code() not in RETRYABLE or attempt == attempts:
                raise
            pause = backoff[attempt - 1]
            log.warning("attempt %d/%d failed with %s, retrying in %.1fs",
                        attempt, attempts, err.code().name, pause)
            time.sleep(pause)


class CounterClient:
    def __init__(self, target="localhost:50051", timeout=TIMEOUT_S, backoff=BACKOFF_S):
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._timeout = timeout
        self._backoff = backoff

    def incr(self, counter_id, delta, key=None):
        # One key per logical operation: generated here, before the first
        # attempt, and reused by every retry inside call_with_retry.
        if key is None:
            key = str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key)
        return call_with_retry(self._stub.Increment, request, self._timeout, self._backoff)

    def get(self, counter_id):
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return call_with_retry(self._stub.Get, request, self._timeout, self._backoff)

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--target", default="localhost:50051", help="host:port of the replica")
    sub = parser.add_subparsers(dest="command", required=True)

    p_incr = sub.add_parser("incr", help="increment a counter")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1, help="delta to add")
    p_incr.add_argument("--key", help="idempotency key (default: a new uuid4)")

    p_get = sub.add_parser("get", help="read a counter")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    with CounterClient(args.target) as client:
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
