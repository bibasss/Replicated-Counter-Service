"""Task C3: failure-injection tests.

Every replica is a real process (subprocess.Popen) on its own port. Crashes
are produced either by proc.kill() while a request is in flight (route a) or
by the replica's --fault flag (route b). Each replica's event log is kept in
logs/test_failures/<test>/ as evidence for the report.
"""
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time

import grpc
import pytest

from client import CounterClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVIDENCE_DIR = os.path.join(ROOT, "logs", "test_failures")


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


class Replica:
    def __init__(self, name, log_dir, *flags):
        self.name = name
        self.port = free_port()
        self.target = f"localhost:{self.port}"
        self._log = open(os.path.join(log_dir, f"{name}.log"), "w")
        self.proc = subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "server.py"), "--port", str(self.port),
             "--name", name, *flags],
            stdout=self._log, stderr=subprocess.STDOUT)
        with grpc.insecure_channel(self.target) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)

    def kill(self):
        self.proc.kill()
        self.proc.wait()

    def stop(self):
        if self.proc.poll() is None:
            self.kill()
        self._log.close()


@pytest.fixture
def replicas(request):
    """Factory: replicas(B=["--fault", "crash-after-apply"]) starts A, B, C."""
    log_dir = os.path.join(EVIDENCE_DIR, request.node.name)
    shutil.rmtree(log_dir, ignore_errors=True)
    os.makedirs(log_dir)
    started = []

    def start(**flags):
        for name in ("A", "B", "C"):
            started.append(Replica(f"replica-{name}", log_dir, *flags.get(name, [])))
        return started

    yield start
    for r in started:
        r.stop()


def value_on(replica, counter_id):
    with CounterClient([replica.target], timeout=1, backoff=()) as c:
        return c.get(counter_id).value


# --- mandated scenarios -----------------------------------------------------

def test_replica_crash_mid_request(replicas):
    # B answers after 3 s, so the write is still in flight there when we kill it.
    a, b, c = replicas(B=["--delay-ms", "3000"])
    client = CounterClient([a.target, b.target, c.target])
    outcome = {}

    def write():
        try:
            outcome["result"] = client.incr("x", 1, wait_all=True)
        except Exception as exc:              # must never happen
            outcome["error"] = exc

    t = threading.Thread(target=write)
    t.start()
    time.sleep(0.5)
    b.kill()                                 # crash B while the request is in flight
    t.join()
    client.close()

    assert "error" not in outcome            # no exception escapes to the caller
    result = outcome["result"]
    assert result.committed and result.acks == 2 and result.new_value == 1
    assert value_on(a, "x") == value_on(c, "x") == 1


def test_request_duplication(replicas):
    a, b, c = replicas()
    with CounterClient([a.target, b.target, c.target]) as client:
        first = client.incr("x", 5, key="dup-key", wait_all=True)
        second = client.incr("x", 5, key="dup-key", wait_all=True)

    assert first.committed and not first.was_duplicate and first.new_value == 5
    assert second.committed and second.was_duplicate and second.new_value == 5
    assert [value_on(r, "x") for r in (a, b, c)] == [5, 5, 5]   # moved once


def test_timeout_then_retry_moves_once(replicas):
    # Every replica answers the first attempt for a key after 2.5 s, longer
    # than the 2 s client deadline. Only the retries can produce the commit.
    slow = ["--fault", "slow-first", "--delay-ms", "2500"]
    a, b, c = replicas(A=slow, B=slow, C=slow)
    with CounterClient([a.target, b.target, c.target]) as client:
        result = client.incr("x", 1, wait_all=True)

    assert result.committed and result.acks == 3
    assert result.was_duplicate              # every ack came from a retry
    assert [value_on(r, "x") for r in (a, b, c)] == [1, 1, 1]


# --- fault matrix: where exactly the replica dies -------------------------

def test_crash_before_apply(replicas):
    a, b, c = replicas(B=["--fault", "crash-before-apply"])
    with CounterClient([a.target, b.target, c.target]) as client:
        result = client.incr("x", 1, wait_all=True)

    assert result.committed and result.acks == 2
    assert b.proc.wait(timeout=5) != 0       # B really crashed
    assert value_on(a, "x") == value_on(c, "x") == 1


def test_crash_after_apply_before_reply(replicas):
    a, b, c = replicas(B=["--fault", "crash-after-apply"])
    with CounterClient([a.target, b.target, c.target]) as client:
        result = client.incr("x", 1, wait_all=True)

    assert result.committed and result.acks == 2
    assert b.proc.wait(timeout=5) != 0
    assert value_on(a, "x") == value_on(c, "x") == 1


def test_two_replicas_crash_no_commit(replicas):
    a, b, c = replicas()
    b.kill()
    c.kill()
    with CounterClient([a.target, b.target, c.target]) as client:
        result = client.incr("x", 1, wait_all=True)

    assert not result.committed and result.acks == 1 and result.new_value is None
    assert value_on(a, "x") == 1             # partially applied, but not committed


def test_committed_writes_survive_single_crash(replicas):
    # 3 runs x 30 writes; in each run a random replica is killed at a random
    # point. Every committed write must be present on both survivors.
    rng = random.Random(42)
    for run in range(3):
        live = replicas()[-3:]
        victim = rng.randrange(3)
        kill_at = rng.randrange(5, 25)
        committed = 0
        with CounterClient([r.target for r in live]) as client:
            for i in range(30):
                if i == kill_at:
                    live[victim].kill()
                if client.incr(f"run{run}", 1).committed:
                    committed += 1
        survivors = [r for j, r in enumerate(live) if j != victim]

        assert committed == 30
        assert [value_on(r, f"run{run}") for r in survivors] == [committed, committed]
