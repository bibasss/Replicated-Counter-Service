"""Task B2 scenario: three replicas, three concurrent clients, Lamport traces.

  client-1: increments counter x twice
  client-2: increments counter y twice   (concurrently with client-1)
  client-3: only issues Get calls

Each process (replica-A/B/C, client-1/2/3) has its own Lamport clock and its
own log file in logs/b2/. The files are then merged into logs/b2/merged.log,
sorted by Lamport value (ties broken by process name), and the lines that
touch replica-A are written to logs/b2/excerpt.log for the report.

Run from the repository root:
    python3 tests/lamport_scenario.py
"""
import os
import re
import subprocess
import sys
import threading
import time

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from client import CounterClient  # noqa: E402

LOG_DIR = os.path.join(ROOT, "logs", "b2")
REPLICAS = {"replica-A": 50051, "replica-B": 50052, "replica-C": 50053}
TARGETS = ",".join(f"localhost:{port}" for port in REPLICAS.values())


def start_replicas():
    procs = []
    for name, port in REPLICAS.items():
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "server.py"), "--port", str(port),
             "--name", name, "--quiet", "--log-file", os.path.join(LOG_DIR, f"{name}.log")],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for port in REPLICAS.values():
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
    return procs


def run_client(name, actions):
    with CounterClient(TARGETS, name, log_path=os.path.join(LOG_DIR, f"{name}.log")) as client:
        for action, counter_id in actions:
            if action == "incr":
                client.incr(counter_id, 1, wait_all=True)
            else:
                client.get(counter_id)


def merge():
    lines = []
    for file_name in sorted(os.listdir(LOG_DIR)):
        if file_name.startswith(("replica-", "client-")):
            with open(os.path.join(LOG_DIR, file_name)) as f:
                lines += [line.rstrip("\n") for line in f if line.strip()]

    def key(line):
        return int(re.search(r" L=(\d+)", line).group(1)), line.split("]")[0]

    lines.sort(key=key)
    with open(os.path.join(LOG_DIR, "merged.log"), "w") as f:
        f.write("\n".join(lines) + "\n")
    excerpt = [l for l in lines if "replica-A" in l or "localhost:50051" in l]
    with open(os.path.join(LOG_DIR, "excerpt.log"), "w") as f:
        f.write("\n".join(excerpt) + "\n")
    return lines, excerpt


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    for file_name in os.listdir(LOG_DIR):
        os.remove(os.path.join(LOG_DIR, file_name))

    procs = start_replicas()
    try:
        clients = [
            threading.Thread(target=run_client, args=("client-1", [("incr", "x"), ("incr", "x")])),
            threading.Thread(target=run_client, args=("client-2", [("incr", "y"), ("incr", "y")])),
            threading.Thread(target=run_client, args=("client-3", [("get", "x"), ("get", "y")])),
        ]
        for t in clients:
            t.start()
        for t in clients:
            t.join()
        time.sleep(0.2)          # let replicas flush their last lines
    finally:
        for p in procs:
            p.terminate()
            p.wait()

    lines, excerpt = merge()
    print("\n".join(lines))
    print(f"\n{len(lines)} events merged into logs/b2/merged.log, "
          f"{len(excerpt)} lines in logs/b2/excerpt.log")


if __name__ == "__main__":
    main()
