"""Task C2: unit and integration tests.

Replicas run in-process on ephemeral ports (port 0) and are stopped after
each test. Clients use short deadlines/backoff so failure paths stay fast.
"""
import threading

import grpc
import pytest

import counter_pb2
import counter_pb2_grpc
from client import CounterClient
from server import serve

FAST = dict(timeout=0.5, backoff=(0.05, 0.1, 0.2))


class Cluster:
    """N in-process replicas plus helpers to build clients and stop replicas."""

    def __init__(self, n, **server_kwargs):
        self.servers = []
        self.targets = []
        for i in range(n):
            server, port = serve(0, name=f"replica-{'ABC'[i]}", **server_kwargs)
            self.servers.append(server)
            self.targets.append(f"localhost:{port}")
        self._clients = []

    def new_client(self, targets=None, **kwargs):
        client = CounterClient(targets or self.targets, **{**FAST, **kwargs})
        self._clients.append(client)
        return client

    def replica_client(self, i):
        """Client that talks to replica i only (to inspect its state)."""
        return self.new_client([self.targets[i]])

    def stop(self, i):
        self.servers[i].stop(0).wait()

    def shutdown(self):
        for client in self._clients:
            client.close()
        for server in self.servers:
            server.stop(0)


@pytest.fixture
def running_server():
    cluster = Cluster(1)
    yield cluster
    cluster.shutdown()


@pytest.fixture
def cluster3():
    cluster = Cluster(3)
    yield cluster
    cluster.shutdown()


# --- single replica ---------------------------------------------------------

def test_increment_applies_delta(running_server):
    client = running_server.new_client()

    reply = client.incr("x", 5)

    assert reply.committed and reply.new_value == 5
    assert client.get("x").value == 5


def test_duplicate_key_not_reapplied(running_server):
    client = running_server.new_client()

    r1 = client.incr("x", 5, key="k-1")
    r2 = client.incr("x", 5, key="k-1")      # retry with the same key

    assert r1.new_value == 5 and not r1.was_duplicate
    assert r2.new_value == 5 and r2.was_duplicate   # not 10
    assert client.get("x").value == 5


def test_concurrent_increments_exact(running_server):
    def work():
        client = running_server.new_client()
        for _ in range(1000):
            assert client.incr("hot", 1).committed

    threads = [threading.Thread(target=work) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert running_server.new_client().get("hot").value == 2000


def test_get_missing_counter(running_server):
    client = running_server.new_client()

    reply = client.get("does-not-exist")

    assert reply.found is False
    assert reply.value == 0


def test_retry_after_timeout_is_safe():
    # The first reply for each key arrives after 1 s, the client deadline is
    # 0.5 s: attempt 1 times out (but was applied), attempt 2 is answered at
    # once from the dedup store.
    cluster = Cluster(1, delay_ms=1000, fault="slow-first")
    try:
        client = cluster.new_client()

        reply = client.incr("x", 1, key="k-timeout")

        assert reply.committed
        assert reply.was_duplicate          # proves the answer came from a retry
        assert reply.new_value == 1
        assert client.get("x").value == 1   # moved exactly once
    finally:
        cluster.shutdown()


# --- three replicas -----------------------------------------------------------

def test_majority_commit_two_acks(cluster3):
    cluster3.stop(1)
    client = cluster3.new_client()

    result = client.incr("x", 1, wait_all=True)

    assert result.committed
    assert result.acks == 2 and result.replicas == 3
    assert result.new_value == 1


def test_no_commit_below_majority(cluster3):
    cluster3.stop(1)
    cluster3.stop(2)
    client = cluster3.new_client()

    result = client.incr("x", 1, wait_all=True)

    assert not result.committed
    assert result.acks == 1
    assert result.new_value is None          # nothing is presented as committed
    # The documented anomaly: the surviving replica did apply the write.
    assert cluster3.replica_client(0).get("x").value == 1


def test_replicas_converge(cluster3):
    def work(counter_id):
        client = cluster3.new_client()
        for _ in range(50):
            assert client.incr(counter_id, 1).committed
        client.close()                       # waits for in-flight calls

    threads = [threading.Thread(target=work, args=(c,)) for c in ("a", "b", "a", "c")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    states = [
        {c: cluster3.replica_client(i).get(c).value for c in ("a", "b", "c")}
        for i in range(3)
    ]
    assert states[0] == states[1] == states[2] == {"a": 100, "b": 50, "c": 50}


# --- additional edge cases -------------------------------------------------

def test_counter_ids_are_isolated(running_server):
    client = running_server.new_client()

    client.incr("likes:post-1", 3)
    client.incr("likes:post-2", 7)

    assert client.get("likes:post-1").value == 3
    assert client.get("likes:post-2").value == 7


def test_retry_storm_same_key_applied_once(cluster3):
    # 20 threads send the same logical operation at the same time.
    results = []

    def send():
        results.append(cluster3.new_client().incr("x", 1, key="storm", wait_all=True))

    threads = [threading.Thread(target=send) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(r.committed and r.new_value == 1 for r in results)
    assert sum(not r.was_duplicate for r in results) <= 3   # one "fresh" ack per replica
    for i in range(3):
        assert cluster3.replica_client(i).get("x").value == 1


def test_request_without_key_is_rejected(running_server):
    with grpc.insecure_channel(running_server.targets[0]) as channel:
        stub = counter_pb2_grpc.CounterStub(channel)
        with pytest.raises(grpc.RpcError) as err:
            stub.Increment(counter_pb2.IncrementRequest(counter_id="x", delta=1), timeout=1)

    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert running_server.new_client().get("x").found is False


def test_negative_delta(running_server):
    client = running_server.new_client()

    client.incr("x", 10)
    reply = client.incr("x", -3)

    assert reply.new_value == 7
