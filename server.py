"""Counter replica: a gRPC server that keeps named counters in memory.

Increment is made safe to retry by a deduplication store keyed by the
client's idempotency key. The store and the counters are protected by one
lock, so "check the key" and "apply the delta" form a single critical section.
"""
import argparse
import logging
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc

log = logging.getLogger("server")


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, delay_ms=0):
        self._lock = threading.Lock()
        self._values = {}        # counter_id -> int
        self._seen = {}          # idempotency_key -> (counter_id, resulting value)
        self._delay_s = delay_ms / 1000.0

    def Increment(self, request, context):
        if not request.counter_id or not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "counter_id and idempotency_key are required")

        with self._lock:
            seen = self._seen.get(request.idempotency_key)
            if seen is not None:
                # Already applied: return the stored result, do not apply again.
                _, value = seen
                reply = counter_pb2.IncrementReply(new_value=value, was_duplicate=True)
            else:
                value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = value
                self._seen[request.idempotency_key] = (request.counter_id, value)
                reply = counter_pb2.IncrementReply(new_value=value, was_duplicate=False)

        log.info("Increment counter=%s delta=%d key=%s -> value=%d duplicate=%s",
                 request.counter_id, request.delta, request.idempotency_key,
                 reply.new_value, reply.was_duplicate)

        # Fault injection: the write is already applied, only the reply is late.
        # This models the "executed, but the reply did not arrive in time" case.
        if self._delay_s:
            time.sleep(self._delay_s)
        return reply

    def Get(self, request, context):
        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)
        return counter_pb2.GetReply(value=value, found=found)


def serve(port, delay_ms=0):
    """Start a server on the given port (0 = pick a free one).

    Returns (server, bound_port) so tests can run it in-process.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(delay_ms), server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port


def main():
    parser = argparse.ArgumentParser(description="Counter replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="delay every Increment reply by this many ms (fault injection)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    server, port = serve(args.port, args.delay_ms)
    log.info("listening on port %d (delay_ms=%d)", port, args.delay_ms)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    main()
