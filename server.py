"""Replica process: hosts two gRPC services on one port.

1. Counter (Assignment 2): named counters with retry-safe increments.
2. KeyValueStore + ReplicaPeer (Lab 6-7): a replicated key-value store with
   configurable read consistency and write strategy, see the KV section below.

Counter service
---------------

Increment is made safe to retry by a deduplication store keyed by the
client's idempotency key. The store and the counters are protected by one
lock, so "check the key" and "apply the delta" form a single critical section.

Every receive, apply and send is a Lamport clock event (see clocks.py).

Fault injection (--fault, used by tests/test_failures.py):
  slow-first          delay only the first reply for each key by --delay-ms
                      (the retry with the same key is answered at once)
  crash-before-apply  the process dies on its first Increment, before applying
  crash-after-apply   the process dies on its first Increment, after applying
                      but before replying
Without --fault, --delay-ms delays every Increment reply.

Key-value store (Lab 6-7)
-------------------------
One node (--leaseholder, node A on port 50051 by default) holds the lease for
the keyspace range [a-z]. All other nodes are followers.

  Get LINEARIZABLE    served only by the leaseholder; a follower proxies it.
                      The leaseholder refuses it once its lease has expired,
                      i.e. it has not heard from a majority for --lease-ms.
  Get FOLLOWER_STALE  served from local state. A follower knows the closed
                      timestamp T sent by the leaseholder's heartbeats: it has
                      every committed CP write with timestamp <= T. If
                      now - T > max_staleness_ms the read fails with
                      FAILED_PRECONDITION "STALE_READ_EXCEEDED".
  Put QUORUM_CP       forwarded to the leaseholder, which replicates to every
                      peer and commits only with a majority (N/2 + 1) of acks.
                      Without a majority nothing is applied and the reply is
                      success=false, "ReplicaUnavailableError: quorum lost".
  Put ALWAYS_AP       applied on the receiving node at once and sent to the
                      peers it can reach; succeeds with any number of acks.

Every value carries a version (timestamp in microseconds, origin node).
Replicas merge versions by Last-Write-Wins: larger timestamp wins, the origin
name breaks ties. Anti-entropy (every --anti-entropy-ms) exchanges the full
state with each peer, so divergent AP writes converge after a partition heals.
"""
import argparse
import logging
import os
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
import kvstore_pb2
import kvstore_pb2_grpc
from clocks import LamportClock

log = logging.getLogger("server")


def _sender(context):
    """Name of the calling client, sent by the client as gRPC metadata."""
    return dict(context.invocation_metadata()).get("client-name", "?")


FAULTS = ("none", "slow-first", "crash-before-apply", "crash-after-apply")


def _crash():
    # Simulate a process crash: no cleanup, no reply, state is lost.
    os._exit(1)


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica", clock=None, delay_ms=0, fault="none"):
        self._name = name
        self._fault = fault
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

        if self._fault == "crash-before-apply":
            _crash()

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

        if self._fault == "crash-after-apply":
            _crash()

        # Fault injection: the write is already applied, only the reply is late.
        # This models the "executed, but the reply did not arrive in time" case.
        if self._delay_s and (self._fault != "slow-first" or not duplicate):
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




# ---------------------------------------------------------------------------
# Key-value store (Lab 6-7)
# ---------------------------------------------------------------------------

KEYSPACE = ("a", "z")    # the single range [a-z]; its lease is held by --leaseholder

# Peer channels reconnect quickly once a partition heals (gRPC's default
# reconnect backoff grows to minutes).
PEER_CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]

CP = kvstore_pb2.QUORUM_CP
LINEARIZABLE = kvstore_pb2.LINEARIZABLE


def now_us():
    return time.time_ns() // 1000


def version(entry):
    """LWW order: larger timestamp wins, the origin node name breaks ties."""
    return entry.timestamp, entry.origin


class HybridClock:
    """Wall-clock microseconds that never go backwards and never repeat.
    observe() moves the clock past timestamps seen in other nodes' writes."""

    def __init__(self):
        self._last = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._last = max(self._last + 1, now_us())
            return self._last

    def observe(self, ts):
        with self._lock:
            self._last = max(self._last, ts)


class _PendingPut:
    """An in-progress Put, so a concurrent retry with the same idempotency
    key waits for the first attempt instead of applying the write again."""

    def __init__(self):
        self.done = threading.Event()
        self.reply = None


class KVNode(kvstore_pb2_grpc.KeyValueStoreServicer, kvstore_pb2_grpc.ReplicaPeerServicer):
    def __init__(self, name, peers=None, leaseholder=None, heartbeat_ms=100,
                 lease_ms=1000, anti_entropy_ms=500, peer_timeout_ms=500):
        self.name = name
        self.peers = dict(peers or {})           # peer name -> "host:port"
        self.leaseholder = leaseholder or name
        if self.leaseholder != name and self.leaseholder not in self.peers:
            raise ValueError(f"leaseholder {self.leaseholder!r} is neither this node nor a peer")
        self.is_leaseholder = self.leaseholder == name
        self.n = len(self.peers) + 1
        self.majority = self.n // 2 + 1
        self._timeout = peer_timeout_ms / 1000.0
        self._heartbeat_s = heartbeat_ms / 1000.0
        self._lease_s = lease_ms / 1000.0
        self._anti_entropy_s = anti_entropy_ms / 1000.0

        self._clock = HybridClock()
        self._lock = threading.Lock()            # protects every field below
        self._data = {}                          # key -> Entry (current version)
        self._requests = {}                      # idempotency_key -> _PendingPut
        # Leaseholder only:
        self._pending = {p: {} for p in self.peers}  # peer -> {key: Entry} not yet delivered
        self._inflight = {}                      # ts -> count of CP writes being replicated
        self._max_closed = 0                     # largest closed timestamp sent so far
        self._lease_acks = {}                    # peer -> monotonic time of last acked heartbeat
        self._lease_was_valid = None
        # Follower only:
        self._closed_ts = 0                      # has every committed CP write with ts <= this

        self._channels = {p: grpc.insecure_channel(a, options=PEER_CHANNEL_OPTIONS)
                          for p, a in self.peers.items()}
        self._peer_stubs = {p: kvstore_pb2_grpc.ReplicaPeerStub(ch)
                            for p, ch in self._channels.items()}
        self._kv_stubs = {p: kvstore_pb2_grpc.KeyValueStoreStub(ch)
                          for p, ch in self._channels.items()}
        self._stop = threading.Event()

    # --- background loops -------------------------------------------------

    def start(self):
        if self.is_leaseholder and self._heartbeat_s:
            for peer in self.peers:
                self._spawn(self._heartbeat_s, self._send_heartbeat, peer)
        if self._anti_entropy_s:
            for peer in self.peers:
                self._spawn(self._anti_entropy_s, self._anti_entropy, peer)

    def stop(self):
        self._stop.set()
        for channel in self._channels.values():
            channel.close()

    def _spawn(self, interval, fn, peer):
        def loop():
            while not self._stop.wait(interval):
                try:
                    fn(peer)
                except grpc.RpcError:
                    pass                         # peer unreachable: try again next round
                except Exception:                # never let a loop die silently
                    log.exception("%s: %s(%s) failed", self.name, fn.__name__, peer)
        threading.Thread(target=loop, daemon=True, name=f"{fn.__name__}-{peer}").start()

    def _send_heartbeat(self, peer):
        with self._lock:
            entries = list(self._pending[peer].values())
            # Closed timestamp: no CP write at or below it is still in flight.
            closed = min(self._inflight) - 1 if self._inflight else self._clock.tick()
            self._max_closed = max(self._max_closed, closed)
        sent_at = time.monotonic()
        try:
            self._peer_stubs[peer].Heartbeat(
                kvstore_pb2.HeartbeatRequest(leaseholder=self.name, closed_timestamp=closed,
                                             entries=entries),
                timeout=self._timeout)
        except grpc.RpcError:
            self._log_lease_change()
            raise
        with self._lock:
            self._lease_acks[peer] = sent_at
            for e in entries:
                queued = self._pending[peer].get(e.key)
                if queued is not None and version(queued) == version(e):
                    del self._pending[peer][e.key]
        if entries:
            log.info("%s: delivered %d queued write(s) to %s", self.name, len(entries), peer)
        self._log_lease_change()

    def _anti_entropy(self, peer):
        with self._lock:
            entries = list(self._data.values())
        reply = self._peer_stubs[peer].Sync(
            kvstore_pb2.SyncRequest(node=self.name, entries=entries), timeout=self._timeout)
        self._merge(reply.entries, peer)

    # --- local state --------------------------------------------------------

    def _apply_locked(self, entry):
        current = self._data.get(entry.key)
        if current is not None and version(current) >= version(entry):
            return False
        stored = kvstore_pb2.Entry()
        stored.CopyFrom(entry)
        self._data[entry.key] = stored
        return True

    def _apply(self, entry):
        self._clock.observe(entry.timestamp)
        with self._lock:
            return self._apply_locked(entry)

    def _merge(self, entries, source):
        changed = [e for e in entries if self._apply(e)]
        for e in changed:
            log.info("%s: LWW merge from %s: %s=%r (ts=%d, origin=%s)",
                     self.name, source, e.key, e.value, e.timestamp, e.origin)
        return len(changed)

    def _queue_locked(self, peer, entry):
        queued = self._pending[peer].get(entry.key)
        if queued is None or version(entry) > version(queued):
            self._pending[peer][entry.key] = entry

    def _lease_valid(self):
        if self.n == 1:
            return True
        now = time.monotonic()
        with self._lock:
            fresh = sum(1 for t in self._lease_acks.values() if now - t <= self._lease_s)
        return 1 + fresh >= self.majority

    def _log_lease_change(self):
        valid = self._lease_valid()
        if valid != self._lease_was_valid:
            self._lease_was_valid = valid
            log.info("%s: lease for range [%s-%s] %s", self.name, *KEYSPACE,
                     "valid" if valid else "EXPIRED (no majority)")

    def _replicate_all(self, entry):
        """Send one version to every peer in parallel. Returns {peer: acked}."""
        calls = {p: stub.Replicate.future(entry, timeout=self._timeout)
                 for p, stub in self._peer_stubs.items()}
        acked = {}
        for peer, call in calls.items():
            try:
                call.result()
                acked[peer] = True
            except grpc.RpcError:
                acked[peer] = False
        return acked

    def _local_reply(self, key, read_ts, stale):
        with self._lock:
            entry = self._data.get(key)
        return kvstore_pb2.GetReply(value=entry.value if entry else "", found=entry is not None,
                                    read_timestamp=read_ts, is_stale=stale)

    @staticmethod
    def _check_key(key, context):
        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "key is required")
        if not KEYSPACE[0] <= key[0] <= KEYSPACE[1]:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"key {key!r} is outside the keyspace range [{KEYSPACE[0]}-{KEYSPACE[1]}]")

    def _forward_error(self, err, what, context):
        """A follower could not complete a call at the leaseholder."""
        if err.details() and err.details().startswith("ReplicaUnavailableError"):
            context.abort(err.code(), err.details())     # the leaseholder's own refusal
        context.abort(grpc.StatusCode.UNAVAILABLE,
                      f"ReplicaUnavailableError: leaseholder {self.leaseholder} is unreachable "
                      f"from {self.name} ({err.code().name}); {what} rejected")

    # --- KeyValueStore ------------------------------------------------------

    def Get(self, request, context):
        self._check_key(request.key, context)

        if request.consistency == LINEARIZABLE:
            if not self.is_leaseholder:
                try:
                    return self._kv_stubs[self.leaseholder].Get(request, timeout=self._timeout)
                except grpc.RpcError as err:
                    self._forward_error(err, "linearizable read", context)
            if not self._lease_valid():
                context.abort(grpc.StatusCode.UNAVAILABLE,
                              f"ReplicaUnavailableError: lease expired: leaseholder {self.name} "
                              f"has not reached a majority for {self._lease_s * 1000:.0f} ms; "
                              f"linearizable read rejected")
            return self._local_reply(request.key, self._clock.tick(), stale=False)

        # FOLLOWER_STALE
        if self.is_leaseholder:
            return self._local_reply(request.key, self._clock.tick(), stale=False)
        with self._lock:
            closed = self._closed_ts
        bound = request.max_staleness_ms
        lag_ms = (now_us() - closed) / 1000.0 if closed else float("inf")
        if bound and lag_ms > bound:
            lag = f"{lag_ms:.0f} ms" if closed else "no heartbeat received yet"
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"STALE_READ_EXCEEDED: {self.name} lags behind leaseholder "
                          f"{self.leaseholder} ({lag}, max_staleness_ms={bound})")
        return self._local_reply(request.key, closed, stale=True)

    def Put(self, request, context):
        self._check_key(request.key, context)
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")

        with self._lock:
            pending = self._requests.get(request.idempotency_key)
            owner = pending is None
            if owner:
                pending = self._requests[request.idempotency_key] = _PendingPut()
        if not owner:
            # Same key seen before: return its result, do not apply again.
            pending.done.wait()
            return pending.reply

        reply = kvstore_pb2.PutReply(success=False, error_message="internal error")
        try:
            reply = self._put_cp(request) if request.strategy == CP else self._put_ap(request)
        finally:
            with self._lock:
                if not reply.success:            # a failed write may be retried later
                    self._requests.pop(request.idempotency_key, None)
            pending.reply = reply
            pending.done.set()
        return reply

    def _put_cp(self, request):
        if not self.is_leaseholder:
            # Only the leaseholder coordinates CP writes for the range.
            try:
                return self._kv_stubs[self.leaseholder].Put(request, timeout=3 * self._timeout)
            except grpc.RpcError as err:
                log.info("%s: CP put %s rejected: leaseholder unreachable", self.name, request.key)
                return kvstore_pb2.PutReply(
                    success=False, acks_received=0,
                    error_message=f"ReplicaUnavailableError: quorum lost: leaseholder "
                                  f"{self.leaseholder} is unreachable from {self.name} "
                                  f"({err.code().name}); CP write rejected")

        with self._lock:
            # Timestamp assignment and the in-flight mark are atomic, so a
            # heartbeat can never close a timestamp a CP write still needs.
            # A CP write is never placed at or below a closed timestamp.
            ts = request.timestamp
            if ts <= self._max_closed:
                ts = self._clock.tick()
            self._clock.observe(ts)
            self._inflight[ts] = self._inflight.get(ts, 0) + 1
        entry = kvstore_pb2.Entry(key=request.key, value=request.value, timestamp=ts,
                                  origin=self.name)
        acked = self._replicate_all(entry)
        acks = 1 + sum(acked.values())
        committed = acks >= self.majority
        with self._lock:
            if committed:
                self._apply_locked(entry)
                for peer, ok in acked.items():
                    if not ok:
                        self._queue_locked(peer, entry)   # delivered by the next heartbeat
            self._inflight[ts] -= 1
            if not self._inflight[ts]:
                del self._inflight[ts]

        if committed:
            log.info("%s: CP put %s=%r committed, acks %d/%d", self.name, request.key,
                     request.value, acks, self.n)
            return kvstore_pb2.PutReply(success=True, acks_received=acks)
        log.info("%s: CP put %s rejected, quorum lost (%d/%d)", self.name, request.key,
                 acks, self.n)
        return kvstore_pb2.PutReply(
            success=False, acks_received=acks - 1,        # replicas that hold the write
            error_message=f"ReplicaUnavailableError: quorum lost: {acks}/{self.n} replicas "
                          f"reachable, need {self.majority}; CP write rejected")

    def _put_ap(self, request):
        ts = request.timestamp or self._clock.tick()
        entry = kvstore_pb2.Entry(key=request.key, value=request.value, timestamp=ts,
                                  origin=self.name)
        self._apply(entry)
        acks = 1 + sum(self._replicate_all(entry).values())
        log.info("%s: AP put %s=%r accepted (ts=%d), acks %d/%d", self.name, request.key,
                 request.value, ts, acks, self.n)
        return kvstore_pb2.PutReply(success=True, acks_received=acks)

    # --- ReplicaPeer --------------------------------------------------------

    def Replicate(self, request, context):
        return kvstore_pb2.ReplicateReply(applied=self._apply(request))

    def Heartbeat(self, request, context):
        for e in request.entries:
            self._clock.observe(e.timestamp)
        with self._lock:
            for e in request.entries:
                self._apply_locked(e)
            self._closed_ts = max(self._closed_ts, request.closed_timestamp)
        return kvstore_pb2.HeartbeatReply(node=self.name)

    def Sync(self, request, context):
        self._merge(request.entries, request.node)
        with self._lock:
            entries = list(self._data.values())
        return kvstore_pb2.SyncReply(node=self.name, entries=entries)

    def ReadLocal(self, request, context):
        with self._lock:
            entry = self._data.get(request.key)
        if entry is None:
            return kvstore_pb2.ReadLocalReply(found=False)
        return kvstore_pb2.ReadLocalReply(found=True, entry=entry)


# ---------------------------------------------------------------------------
# Process entry point
# ---------------------------------------------------------------------------

def serve(port, name="replica", clock=None, delay_ms=0, fault="none", kv_options=None):
    """Start a server on the given port (0 = pick a free one) with both the
    Counter and the KeyValueStore services.

    Returns (server, bound_port) so tests can run it in-process.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    counter = CounterServicer(name, clock, delay_ms, fault)
    counter_pb2_grpc.add_CounterServicer_to_server(counter, server)
    node = KVNode(name, **(kv_options or {}))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(node, server)
    kvstore_pb2_grpc.add_ReplicaPeerServicer_to_server(node, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    node.start()
    return server, bound_port


def parse_peers(text):
    """"B=localhost:50052,C=localhost:50053" -> {"B": "localhost:50052", ...}"""
    peers = {}
    for item in filter(None, (text or "").split(",")):
        name, sep, addr = item.partition("=")
        if not sep or not name or not addr:
            raise argparse.ArgumentTypeError(f"bad peer {item!r}, expected NAME=HOST:PORT")
        peers[name] = addr
    return peers


def main():
    parser = argparse.ArgumentParser(description="Counter / key-value replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", help="replica name used in event logs (default: replica-<port>)")
    parser.add_argument("--log-file", help="append Lamport event lines to this file")
    parser.add_argument("--quiet", action="store_true", help="do not print event lines to stderr")
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="delay Increment replies by this many ms (fault injection)")
    parser.add_argument("--fault", choices=FAULTS, default="none",
                        help="simulated failure, see module docstring")
    kv = parser.add_argument_group("key-value store (Lab 6-7)")
    kv.add_argument("--peers", type=parse_peers, default={},
                    help="other nodes as NAME=HOST:PORT,... (empty = single-node cluster)")
    kv.add_argument("--leaseholder", help="name of the node holding the lease for [a-z] "
                                          "(default: this node)")
    kv.add_argument("--heartbeat-ms", type=int, default=100,
                    help="leaseholder heartbeat / closed-timestamp interval (0 = off)")
    kv.add_argument("--lease-ms", type=int, default=1000,
                    help="lease is valid while a majority acked a heartbeat this recently")
    kv.add_argument("--anti-entropy-ms", type=int, default=500,
                    help="full-state LWW sync with every peer at this interval (0 = off)")
    kv.add_argument("--peer-timeout-ms", type=int, default=500,
                    help="deadline for replica-to-replica calls")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    name = args.name or f"replica-{args.port}"
    clock = LamportClock(name, args.log_file, echo=not args.quiet)
    kv_options = dict(peers=args.peers, leaseholder=args.leaseholder,
                      heartbeat_ms=args.heartbeat_ms, lease_ms=args.lease_ms,
                      anti_entropy_ms=args.anti_entropy_ms, peer_timeout_ms=args.peer_timeout_ms)
    server, port = serve(args.port, name, clock, args.delay_ms, args.fault, kv_options)
    log.info("%s listening on port %d (delay_ms=%d, fault=%s, leaseholder=%s, peers=%s)",
             name, port, args.delay_ms, args.fault, args.leaseholder or name,
             ",".join(args.peers) or "-")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)
    finally:
        clock.close()


if __name__ == "__main__":
    main()
