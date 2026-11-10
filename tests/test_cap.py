"""Lab 6-7: quorum arithmetic, CP vs AP under network partitions, bounded staleness.

Failure injection is socket interception (netsim.py): every replica is a real
process, and every replica-to-replica link goes through a TCP proxy owned by
the test. Cutting proxies splits the cluster into DC-1 (A, B) and DC-2 (C):

        client-1 --> A (leaseholder) <--> B          C <-- client-2
                     |____ DC-1 ____|   ||cut||   DC-2

Clients talk to "their" replica directly. Each test gets a fresh cluster;
every replica's log is kept in logs/test_cap/<test name>/.
"""
import os
import shutil
import time
from itertools import combinations

import grpc
import pytest

import kvstore_pb2
from client import KVClient, QuorumClient, ReplicaUnavailableError, StaleReadExceededError
from netsim import Cluster

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVIDENCE_DIR = os.path.join(ROOT, "logs", "test_cap")
DC1, DC2 = ["A", "B"], ["C"]
N = 3
LEASE_MS = 1000                      # server default --lease-ms


def now_us():
    return time.time_ns() // 1000


def eventually(condition, timeout=5.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        time.sleep(interval)


def evidence_dir(name):
    path = os.path.join(EVIDENCE_DIR, name)
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    return path


@pytest.fixture
def cluster(request):
    """Three replicas A, B, C; A holds the lease. Extra replica flags come from
    @pytest.mark.server_flags(...)."""
    marker = request.node.get_closest_marker("server_flags")
    c = Cluster(log_dir=evidence_dir(request.node.name),
                server_flags=marker.args if marker else ())
    clients = {}

    def client(name):
        if name not in clients:
            clients[name] = KVClient(c.target(name), timeout=2, backoff=())
        return clients[name]

    c.client = client
    yield c
    for kv in clients.values():
        kv.close()
    c.stop()


# ---------------------------------------------------------------------------
# 1. Quorum arithmetic: R + W > N vs R + W <= N
# ---------------------------------------------------------------------------
# Dynamo-style client quorums over the replicas' internal API, with
# anti-entropy off so a version exists only where the client wrote it. For
# every choice of W write replicas and R read replicas: write "old" to all
# three, write "new" to the W replicas, read the R replicas, keep the newest.

QUORUM_ROWS = []


@pytest.fixture(scope="module")
def quorum():
    c = Cluster(log_dir=evidence_dir("quorum_arithmetic"),
                server_flags=["--anti-entropy-ms", "0"])
    q = QuorumClient([c.target(n) for n in c.names], timeout=2)
    yield q
    q.close()
    c.stop()
    with open(os.path.join(EVIDENCE_DIR, "quorum_matrix.md"), "w") as f:
        f.write("| W | R | R+W>N | combinations | fresh reads | stale reads | "
                "stale exactly when write set and read set are disjoint |\n"
                "|---|---|---|---|---|---|---|\n")
        for row in QUORUM_ROWS:
            f.write("| {w} | {r} | {overlap} | {total} | {fresh} | {stale} | {exact} |\n"
                    .format(**row))


def all_combinations(q, w, r):
    results = []
    for ws in combinations(range(N), w):
        for rs in combinations(range(N), r):
            key = f"q-w{w}r{r}-{''.join(map(str, ws))}-{''.join(map(str, rs))}"
            base = now_us()
            assert q.write(key, "old", range(N), timestamp=base) == N
            assert q.write(key, "new", ws, timestamp=base + 1) == w
            got = q.read(key, rs)
            assert got.replies == r
            results.append((set(ws), set(rs), got.value == "new"))
    exact = all(fresh == bool(ws & rs) for ws, rs, fresh in results)
    QUORUM_ROWS.append(dict(
        w=w, r=r, overlap="yes" if w + r > N else "no", total=len(results),
        fresh=sum(f for *_, f in results), stale=sum(not f for *_, f in results),
        exact="yes" if exact else "NO"))
    return results


@pytest.mark.parametrize("w,r", [(2, 2), (3, 1), (1, 3)])
def test_quorum_overlap_read_always_sees_latest_write(quorum, w, r):
    assert w + r > N
    results = all_combinations(quorum, w, r)
    # Pigeonhole: |W| + |R| > N forces a common replica, which has "new".
    assert all(ws & rs for ws, rs, _ in results)
    assert all(fresh for *_, fresh in results)


@pytest.mark.parametrize("w,r", [(1, 1), (1, 2), (2, 1)])
def test_quorum_without_overlap_allows_stale_read(quorum, w, r):
    assert w + r <= N
    results = all_combinations(quorum, w, r)
    # A read is stale exactly when the read set misses every replica written.
    assert all(fresh == bool(ws & rs) for ws, rs, fresh in results)
    assert any(not fresh for *_, fresh in results)


# ---------------------------------------------------------------------------
# 2. Task 3 Part A: CP mode (strict quorum) under partition DC-1 | DC-2
# ---------------------------------------------------------------------------

def test_cp_write_on_majority_partition_succeeds(cluster):
    cluster.partition(DC1, DC2)

    reply = cluster.client("A").put("user", "alice", kvstore_pb2.QUORUM_CP)
    assert reply.success and reply.acks_received == 2

    # Through the follower of DC-1: forwarded to the leaseholder, same quorum.
    reply = cluster.client("B").put("user", "bob", kvstore_pb2.QUORUM_CP)
    assert reply.success and reply.acks_received == 2

    assert cluster.client("B").get("user", kvstore_pb2.LINEARIZABLE).value == "bob"
    assert cluster.local_value("A", "user") == "bob"
    assert cluster.local_value("B", "user") == "bob"
    assert cluster.local_value("C", "user") is None        # cut off, never received it


def test_cp_write_on_minority_partition_rejected(cluster):
    cluster.partition(DC1, DC2)

    with pytest.raises(ReplicaUnavailableError, match="quorum lost") as info:
        cluster.client("C").put("user", "carol", kvstore_pb2.QUORUM_CP)
    reply = info.value.reply
    assert not reply.success and reply.acks_received == 0
    assert "ReplicaUnavailableError" in reply.error_message

    # Rejected means not applied anywhere: not on C, and not after healing.
    assert cluster.local_value("C", "user") is None
    cluster.heal()
    time.sleep(1.0)                                        # two anti-entropy rounds
    assert all(cluster.local_value(n, "user") is None for n in "ABC")


# ---------------------------------------------------------------------------
# 3. Task 3 Part B: AP mode (sloppy quorum, LWW) and partition recovery
# ---------------------------------------------------------------------------

def test_ap_write_on_minority_partition_accepted(cluster):
    cluster.partition(DC1, DC2)

    reply = cluster.client("C").put("cart", "dc2-item", kvstore_pb2.ALWAYS_AP)
    assert reply.success and reply.acks_received == 1
    assert cluster.local_value("C", "cart") == "dc2-item"
    assert cluster.local_value("A", "cart") is None
    assert cluster.local_value("B", "cart") is None


def test_partition_recovery_merges_divergent_values_by_lww(cluster):
    cluster.partition(DC1, DC2)
    base = now_us()
    a, c = cluster.client("A"), cluster.client("C")

    # "cart": DC-1 writes first, DC-2 writes later with a larger timestamp.
    assert a.put("cart", "dc1-version", kvstore_pb2.ALWAYS_AP, timestamp=base + 1000).success
    assert c.put("cart", "dc2-version", kvstore_pb2.ALWAYS_AP, timestamp=base + 2000).success
    # "profile": DC-2 writes last in real time but its clock is behind, so its
    # timestamp is older. LWW orders by timestamp, not by arrival.
    assert a.put("profile", "dc1-version", kvstore_pb2.ALWAYS_AP, timestamp=base + 3000).success
    assert c.put("profile", "dc2-version", kvstore_pb2.ALWAYS_AP, timestamp=base + 2500).success

    # Divergence while partitioned.
    assert [cluster.local_value(n, "cart") for n in "ABC"] == ["dc1-version"] * 2 + ["dc2-version"]
    assert [cluster.local_value(n, "profile") for n in "ABC"] == ["dc1-version"] * 2 + ["dc2-version"]

    cluster.heal()
    eventually(lambda: all(cluster.local_value(n, "cart") == "dc2-version" and
                           cluster.local_value(n, "profile") == "dc1-version" for n in "ABC"))
    assert a.get("cart", kvstore_pb2.LINEARIZABLE).value == "dc2-version"
    assert c.get("profile", kvstore_pb2.LINEARIZABLE).value == "dc1-version"


# ---------------------------------------------------------------------------
# 4. Task 2: read consistency (leaseholder vs follower) and bounded staleness
# ---------------------------------------------------------------------------

def test_follower_read_is_local_and_fresh_when_connected(cluster):
    assert cluster.client("A").put("price", "10", kvstore_pb2.QUORUM_CP).acks_received == 3

    stale = cluster.client("C").get("price", kvstore_pb2.FOLLOWER_STALE, max_staleness_ms=5000)
    assert stale.found and stale.value == "10" and stale.is_stale

    linear = cluster.client("C").get("price", kvstore_pb2.LINEARIZABLE)  # proxied to A
    assert linear.found and linear.value == "10" and not linear.is_stale
    assert linear.read_timestamp >= stale.read_timestamp


def test_minority_refuses_linearizable_read_and_bounds_follower_read(cluster):
    assert cluster.client("A").put("balance", "100", kvstore_pb2.QUORUM_CP).acks_received == 3
    cluster.partition(DC1, DC2)
    assert cluster.client("A").put("balance", "250", kvstore_pb2.QUORUM_CP).acks_received == 2
    c = cluster.client("C")

    # C (consistency): no linearizable read without the leaseholder.
    with pytest.raises(ReplicaUnavailableError, match="leaseholder A is unreachable"):
        c.get("balance", kvstore_pb2.LINEARIZABLE)

    # A (availability): the follower still answers, with the old value.
    reply = c.get("balance", kvstore_pb2.FOLLOWER_STALE, max_staleness_ms=5000)
    assert reply.value == "100" and reply.is_stale

    # ...but only while it lags less than the bound.
    time.sleep(0.5)
    with pytest.raises(StaleReadExceededError, match="STALE_READ_EXCEEDED"):
        c.get("balance", kvstore_pb2.FOLLOWER_STALE, max_staleness_ms=300)


@pytest.mark.server_flags("--anti-entropy-ms", "0")
def test_heartbeat_delivers_missed_cp_writes_within_staleness_bound(cluster):
    """Anti-entropy is off: C catches up only through the leaseholder's
    heartbeats. Once a follower read within the bound succeeds, it must
    include every CP write committed longer ago than the bound."""
    cluster.partition(DC1, DC2)
    assert cluster.client("A").put("config", "v2", kvstore_pb2.QUORUM_CP).acks_received == 2
    time.sleep(1.2)                                   # the write is now > 1000 ms old
    assert cluster.local_value("C", "config") is None
    cluster.heal()

    values = []

    def read_on_c():
        try:
            values.append(cluster.client("C").get(
                "config", kvstore_pb2.FOLLOWER_STALE, max_staleness_ms=1000).value)
            return True
        except StaleReadExceededError:
            return False

    eventually(read_on_c)
    assert values == ["v2"]                           # never the pre-partition state


# ---------------------------------------------------------------------------
# 5. Split-brain mitigation, crashes, retries
# ---------------------------------------------------------------------------

def test_isolated_leaseholder_loses_lease_no_split_brain(cluster):
    cluster.partition(["A"], ["B", "C"])
    time.sleep(LEASE_MS / 1000 + 0.3)

    with pytest.raises(ReplicaUnavailableError, match="lease expired"):
        cluster.client("A").get("user", kvstore_pb2.LINEARIZABLE)
    with pytest.raises(ReplicaUnavailableError, match="quorum lost"):
        cluster.client("A").put("user", "from-A", kvstore_pb2.QUORUM_CP)
    # The majority side cannot take CP writes either (no lease failover), so
    # there are never two sides committing CP writes at the same time.
    with pytest.raises(ReplicaUnavailableError, match="leaseholder A is unreachable"):
        cluster.client("B").put("user", "from-B", kvstore_pb2.QUORUM_CP)
    # AP keeps accepting on both sides.
    assert cluster.client("B").put("user", "ap-B", kvstore_pb2.ALWAYS_AP).acks_received == 2
    assert cluster.client("A").put("user", "ap-A", kvstore_pb2.ALWAYS_AP).acks_received == 1


def test_cp_write_survives_one_crash_not_two(cluster):
    a = cluster.client("A")
    cluster.kill("C")
    reply = a.put("stock", "7", kvstore_pb2.QUORUM_CP)
    assert reply.success and reply.acks_received == 2

    cluster.kill("B")
    with pytest.raises(ReplicaUnavailableError, match="quorum lost: 1/3"):
        a.put("stock", "6", kvstore_pb2.QUORUM_CP)
    assert cluster.local_value("A", "stock") == "7"


def test_put_retry_with_same_idempotency_key_applied_once(cluster):
    first = cluster.client("B").put("order", "v1", kvstore_pb2.QUORUM_CP,
                                    idempotency_key="order-retry-1")
    assert cluster.client("A").put("order", "v2", kvstore_pb2.QUORUM_CP).success
    # A late retry of the first request through another node: answered from
    # the leaseholder's dedup store, not written again over "v2".
    retry = cluster.client("C").put("order", "v1", kvstore_pb2.QUORUM_CP,
                                    idempotency_key="order-retry-1")
    assert retry == first
    assert cluster.client("A").get("order", kvstore_pb2.LINEARIZABLE).value == "v2"


def test_invalid_requests_rejected(cluster):
    a = cluster.client("A")
    with pytest.raises(grpc.RpcError) as info:
        a.get("Upper", kvstore_pb2.LINEARIZABLE)         # outside range [a-z]
    assert info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as info:
        a.put("", "x")
    assert info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
