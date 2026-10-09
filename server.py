"""Counter replica: a gRPC server that keeps named counters in memory.

Increment is made safe to retry by a deduplication store keyed by the
client's idempotency key. The store and the counters are protected by one
lock, so "check the key" and "apply the delta" form a single critical section.

Every receive, apply and send is a Lamport clock event (see clocks.py).
"""
import argparse
import logging
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

log = logging.getLogger("server")


def _sender(context):
    """Name of the calling client, sent by the client as gRPC metadata."""
    return dict(context.invocation_metadata()).get("client-name", "?")


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica", clock=None, delay_ms=0):
        self._name = name
        self._clock = clock or LamportClock(name)
        self._lock = threading.Lock()
        self._values = {}        # counter_id -> int
        self._seen = {}          # idempotency_key -> (counter_id, resulting value)
        self._delay_s = delay_ms / 1000.0

    def Increment(self, request, context):
        sender = _sender(context)
        self._clock.receive(
            request.lamport_time,
            f"Increment(counter={request.counter_id}, delta={request.delta}) <- {sender}")

        if not request.counter_id or not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "counter_id and idempotency_key are required")

        with self._lock:
            seen = self._seen.get(request.idempotency_key)
            if seen is not None:
                # Already applied: return the stored result, do not apply again.
                _, value = seen
                duplicate = True
                self._clock.event("DUP", f"key={request.idempotency_key[:8]} counter="
                                         f"{request.counter_id} stays {value}")
            else:
                value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = value
                self._seen[request.idempotency_key] = (request.counter_id, value)
                duplicate = False
                self._clock.event("APPLY", f"counter={request.counter_id} -> {value}")

        # Fault injection: the write is already applied, only the reply is late.
        # This models the "executed, but the reply did not arrive in time" case.
        if self._delay_s:
            time.sleep(self._delay_s)

        lamport = self._clock.send(f"IncrementReply(new_value={value}) -> {sender}")
        context.set_trailing_metadata((("replica-name", self._name),))
        return counter_pb2.IncrementReply(
            new_value=value, was_duplicate=duplicate, lamport_time=lamport)

    def Get(self, request, context):
        sender = _sender(context)
        self._clock.receive(request.lamport_time,
                            f"Get(counter={request.counter_id}) <- {sender}")
        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)
        lamport = self._clock.send(f"GetReply(value={value}) -> {sender}")
        context.set_trailing_metadata((("replica-name", self._name),))
        return counter_pb2.GetReply(value=value, found=found, lamport_time=lamport)


def serve(port, name="replica", clock=None, delay_ms=0):
    """Start a server on the given port (0 = pick a free one).

    Returns (server, bound_port) so tests can run it in-process.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(name, clock, delay_ms)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port


def main():
    parser = argparse.ArgumentParser(description="Counter replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", help="replica name used in event logs (default: replica-<port>)")
    parser.add_argument("--log-file", help="append Lamport event lines to this file")
    parser.add_argument("--quiet", action="store_true", help="do not print event lines to stderr")
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="delay every Increment reply by this many ms (fault injection)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    name = args.name or f"replica-{args.port}"
    clock = LamportClock(name, args.log_file, echo=not args.quiet)
    server, port = serve(args.port, name, clock, args.delay_ms)
    log.info("%s listening on port %d (delay_ms=%d)", name, port, args.delay_ms)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)
    finally:
        clock.close()


if __name__ == "__main__":
    main()
