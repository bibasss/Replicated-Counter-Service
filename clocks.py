"""Lamport logical clock shared by the client and the replicas.

Rules:
  * before every local event (SEND, APPLY, ...): L = L + 1
  * on receiving a message carrying R:            L = max(L, R) + 1
The value after the update is attached to outgoing messages and written to
the event log, one line per event:

  [client-1] SEND   Increment(counter=x, delta=1) -> localhost:50051   L=4
  [replica-A] RECV  Increment(counter=x, delta=1) <- client-1          L=5  (received L=4)
"""
import sys
import threading


class LamportClock:
    def __init__(self, name, log_path=None, echo=False):
        self.name = name
        self._time = 0
        # One lock for "update the counter + write the line", so the log of
        # a process is always in increasing L order even with many threads.
        self._lock = threading.Lock()
        self._file = open(log_path, "a", buffering=1) if log_path else None
        self._echo = echo

    @property
    def time(self):
        with self._lock:
            return self._time

    def event(self, kind, text):
        """Local event (APPLY, DUP, ...): tick and log. Returns the new L."""
        with self._lock:
            self._time += 1
            self._write(kind, text, self._time)
            return self._time

    def send(self, text):
        """Send event: tick, log, and return the value to attach to the message."""
        return self.event("SEND", text)

    def receive(self, received, text):
        """Receive event: L = max(L, received) + 1, then log."""
        with self._lock:
            self._time = max(self._time, received) + 1
            self._write("RECV", text, self._time, received)
            return self._time

    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    def _write(self, kind, text, time, received=None):
        if not self._file and not self._echo:
            return
        line = f"[{self.name}] {kind:<6} {text:<62} L={time}"
        if received is not None:
            line += f"  (received L={received})"
        if self._file:
            self._file.write(line + "\n")
        if self._echo:
            print(line, file=sys.stderr, flush=True)
