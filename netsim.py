"""Network simulation for the Lab 6-7 key-value cluster (socket interception).

Every replica is a real process (subprocess.Popen). Replicas never talk to
each other directly: each directed link X -> Y goes through a LinkProxy, a
small TCP proxy owned by this process. Cutting a proxy is a network
partition on that link; a proxy can also add one-way latency (a WAN link).
Clients talk to replicas directly, i.e. a client "lives" in the data centre
of the replica it calls.

    cluster = Cluster(log_dir="logs/x")         # A (leaseholder), B in DC-1; C in DC-2
    cluster.partition(["A", "B"], ["C"])        # DC-1 | DC-2
    cluster.heal()
    cluster.stop()

Run it directly for a manual demo (fixed ports 50051-50053):

    python3 netsim.py            # then type: partition A,B C | heal | quit
"""
import os
import queue
import socket
import subprocess
import sys
import threading
import time

import grpc

import kvstore_pb2
import kvstore_pb2_grpc

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DCS = {"A": "DC-1", "B": "DC-1", "C": "DC-2"}


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def _nodelay(sock):
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


class LinkProxy:
    """TCP proxy for one directed link. cut() drops every open connection and
    refuses new ones until restore(); delay_ms delays each direction."""

    def __init__(self, upstream_port, delay_ms=0, name="link"):
        self.name = name
        self._upstream = ("localhost", upstream_port)
        self._delay_s = delay_ms / 1000.0
        self._lock = threading.Lock()
        self._cut = False
        self._conns = set()
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("localhost", 0))
        self._listener.listen(64)
        self.port = self._listener.getsockname()[1]
        threading.Thread(target=self._accept_loop, daemon=True, name=f"proxy-{name}").start()

    @property
    def is_cut(self):
        return self._cut

    def cut(self):
        with self._lock:
            self._cut = True
            conns, self._conns = self._conns, set()
        for sock in conns:
            self._close(sock)

    def restore(self):
        with self._lock:
            self._cut = False

    def close(self):
        self.cut()
        self._close(self._listener)

    @staticmethod
    def _close(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def _accept_loop(self):
        while True:
            try:
                downstream, _ = self._listener.accept()
            except OSError:
                return                                    # listener closed
            if self._cut:
                self._close(downstream)                   # partitioned: refuse
                continue
            try:
                upstream = socket.create_connection(self._upstream, timeout=2)
                upstream.settimeout(None)
            except OSError:
                self._close(downstream)                   # target process is down
                continue
            _nodelay(downstream)
            _nodelay(upstream)
            with self._lock:
                if self._cut:
                    self._close(downstream)
                    self._close(upstream)
                    continue
                self._conns.update((downstream, upstream))
            for src, dst in ((downstream, upstream), (upstream, downstream)):
                threading.Thread(target=self._pipe, args=(src, dst), daemon=True).start()

    def _pipe(self, src, dst):
        if not self._delay_s:
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    dst.sendall(data)
            except OSError:
                pass
        else:
            # Constant one-way delay: a FIFO of (due time, bytes), sent in order.
            fifo = queue.Queue()
            sender = threading.Thread(target=self._delayed_sender, args=(fifo, dst), daemon=True)
            sender.start()
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    fifo.put((time.monotonic() + self._delay_s, data))
            except OSError:
                pass
            fifo.put(None)
            sender.join()
        for sock in (src, dst):
            self._close(sock)
        with self._lock:
            self._conns.difference_update((src, dst))

    @staticmethod
    def _delayed_sender(fifo, dst):
        while True:
            item = fifo.get()
            if item is None:
                return
            due, data = item
            pause = due - time.monotonic()
            if pause > 0:
                time.sleep(pause)
            try:
                dst.sendall(data)
            except OSError:
                return


class Node:
    def __init__(self, name, port, log_path):
        self.name = name
        self.port = port
        self.target = f"localhost:{port}"
        self.log_path = log_path
        self.proc = None
        self._log = None

    def start(self, args):
        self._log = open(self.log_path, "a") if self.log_path else subprocess.DEVNULL
        self.proc = subprocess.Popen([sys.executable, os.path.join(ROOT, "server.py"), *args],
                                     stdout=self._log, stderr=subprocess.STDOUT, cwd=ROOT)

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def kill(self):
        """Simulated crash: SIGKILL, no cleanup, in-memory state is lost."""
        if self.alive():
            self.proc.kill()
            self.proc.wait()

    def close(self):
        self.kill()
        if self._log not in (None, subprocess.DEVNULL):
            self._log.close()


class Cluster:
    """Replica processes wired together through LinkProxy objects."""

    def __init__(self, names=("A", "B", "C"), leaseholder="A", dcs=None, log_dir=None,
                 wan_delay_ms=0, ports=None, server_flags=(), ready_timeout=15):
        self.names = list(names)
        self.leaseholder = leaseholder
        self.dcs = dict(dcs or DEFAULT_DCS)
        ports = list(ports) if ports else [free_port() for _ in self.names]
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        self.nodes = {
            n: Node(n, p, os.path.join(log_dir, f"replica-{n}.log") if log_dir else None)
            for n, p in zip(self.names, ports)}
        # links[(src, dst)] carries src's calls to dst.
        self.links = {}
        for src in self.names:
            for dst in self.names:
                if src != dst:
                    wan = self.dcs.get(src) != self.dcs.get(dst)
                    self.links[(src, dst)] = LinkProxy(
                        self.nodes[dst].port, wan_delay_ms if wan else 0, f"{src}->{dst}")
        self._server_flags = list(server_flags)
        self._channels = {}
        try:
            for n in self.names:                 # launch all, then wait: faster start
                self.start_node(n, wait=False)
            for n in self.names:
                self._wait_listening(n)
            self.wait_ready(ready_timeout)
        except Exception:
            self.stop()
            raise

    # --- processes -----------------------------------------------------------

    def start_node(self, name, wait=True):
        node = self.nodes[name]
        peers = ",".join(f"{m}=localhost:{self.links[(name, m)].port}"
                         for m in self.names if m != name)
        node.start(["--port", str(node.port), "--name", name, "--quiet",
                    "--leaseholder", self.leaseholder, "--peers", peers, *self._server_flags])
        if wait:
            self._wait_listening(name)

    def _wait_listening(self, name):
        with grpc.insecure_channel(self.target(name)) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)

    def kill(self, name):
        self.nodes[name].kill()

    def target(self, name):
        return self.nodes[name].target

    # --- network ---------------------------------------------------------------

    def partition(self, *groups):
        """partition(["A", "B"], ["C"]): links inside a group work, links
        between groups are cut. Nodes not listed are isolated."""
        group_of = {n: i for i, g in enumerate(groups) for n in g}
        for (src, dst), link in self.links.items():
            same = src in group_of and group_of.get(src) == group_of.get(dst)
            link.restore() if same else link.cut()

    def heal(self):
        for link in self.links.values():
            link.restore()

    # --- state inspection (bypasses the KeyValueStore API) ---------------------

    def _channel(self, name):
        if name not in self._channels:
            self._channels[name] = grpc.insecure_channel(self.target(name))
        return self._channels[name]

    def kv_stub(self, name):
        return kvstore_pb2_grpc.KeyValueStoreStub(self._channel(name))

    def peer_stub(self, name):
        return kvstore_pb2_grpc.ReplicaPeerStub(self._channel(name))

    def local_value(self, name, key):
        """Value stored on one replica (no routing, no consistency checks)."""
        reply = self.peer_stub(name).ReadLocal(kvstore_pb2.ReadLocalRequest(key=key), timeout=2)
        return reply.entry.value if reply.found else None

    def wait_ready(self, timeout=15):
        """Until the leaseholder holds a valid lease and every follower has a
        fresh closed timestamp."""
        deadline = time.monotonic() + timeout
        checks = [(self.leaseholder, kvstore_pb2.LINEARIZABLE, 0)]
        checks += [(n, kvstore_pb2.FOLLOWER_STALE, 1000) for n in self.names
                   if n != self.leaseholder]
        for name, consistency, bound in checks:
            while True:
                try:
                    self.kv_stub(name).Get(kvstore_pb2.GetRequest(
                        key="ready", consistency=consistency, max_staleness_ms=bound), timeout=1)
                    break
                except grpc.RpcError:
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"node {name} not ready after {timeout}s")
                    time.sleep(0.05)

    def stop(self):
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()
        for node in self.nodes.values():
            node.close()
        for link in self.links.values():
            link.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


def main():
    print("starting A (leaseholder, DC-1) :50051, B (DC-1) :50052, C (DC-2) :50053 ...")
    cluster = Cluster(ports=(50051, 50052, 50053), log_dir=os.path.join(ROOT, "logs", "netsim"))
    print("ready. replica logs in logs/netsim/. commands:\n"
          "  partition A,B C   cut every link between the groups\n"
          "  heal              restore every link\n"
          "  kill NAME         crash a replica\n"
          "  quit")
    try:
        for line in sys.stdin:
            if not line.split():
                continue
            cmd, *rest = line.split()
            if cmd == "partition":
                cluster.partition(*(g.split(",") for g in rest))
                print("partitioned:", " | ".join(rest))
            elif cmd == "heal":
                cluster.heal()
                print("healed")
            elif cmd == "kill" and rest:
                cluster.kill(rest[0])
                print("killed", rest[0])
            elif cmd == "quit":
                break
    except KeyboardInterrupt:
        pass
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
