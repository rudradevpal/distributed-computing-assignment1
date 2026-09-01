"""
network.py
----------
One persistent TCP connection per directed channel Pi -> Pj.

Why this matters: Chandy-Lamport assumes FIFO channels. A single TCP connection
per channel, written by exactly one sender thread and read by exactly one reader
thread, gives us that guarantee for free. A marker can never overtake an
application message sent earlier on the same channel.

link_delays lets us slow down an individual channel (e.g. "2->3": 2.0 seconds).
The delay is applied inside the per-channel sender thread, so ordering on that
channel is still strictly FIFO. This is what makes the "message in flight during
the snapshot" test case deterministic instead of a race.
"""

import socket
import threading
import time
import queue

import message


class Network:
    def __init__(self, cfg, idx, inbox, log):
        self.cfg = cfg
        self.idx = idx
        self.inbox = inbox                # queue.Queue of decoded messages
        self.log = log
        self.procs = cfg["processes"]
        self.n = len(self.procs)
        self.link_delays = cfg.get("link_delays", {})

        self.running = True
        self.out_queues = {}              # dst -> queue.Queue
        self.threads = []

        for p in self.procs:
            if p["id"] != self.idx:
                self.out_queues[p["id"]] = queue.Queue()

    # ---------- startup ----------

    def start(self):
        me = self.procs[self.idx]
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("0.0.0.0", me["port"]))
        self.listener.listen(16)

        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()
        self.threads.append(t)

        for dst in self.out_queues:
            t = threading.Thread(target=self._sender_loop, args=(dst,), daemon=True)
            t.start()
            self.threads.append(t)

        self.log("network listening on 0.0.0.0:%d" % me["port"])

    # ---------- receiving ----------

    def _accept_loop(self):
        while self.running:
            try:
                conn, _addr = self.listener.accept()
            except OSError:
                return
            t = threading.Thread(target=self._reader_loop, args=(conn,), daemon=True)
            t.start()

    def _reader_loop(self, conn):
        """One thread per incoming channel: reads lines in order, preserving FIFO."""
        buf = b""
        try:
            while self.running:
                chunk = conn.recv(8192)
                if not chunk:
                    return
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if line.strip():
                        self.inbox.put(message.decode(line))
        except OSError:
            return
        finally:
            try:
                conn.close()
            except OSError:
                pass

    # ---------- sending ----------

    def send(self, msg):
        self.out_queues[msg["dst"]].put(msg)

    def _delay_for(self, dst):
        return float(self.link_delays.get("%d->%d" % (self.idx, dst), 0.0))

    def _connect(self, dst):
        """Retry until the peer is up, so processes may be started in any order."""
        target = self.procs[dst]
        while self.running:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3.0)
                s.connect((target["host"], target["port"]))
                s.settimeout(None)
                self.log("connected to P%d (%s:%d)" % (dst, target["host"], target["port"]))
                return s
            except OSError:
                time.sleep(0.3)
        return None

    def _sender_loop(self, dst):
        sock = None
        delay = self._delay_for(dst)
        while self.running:
            try:
                msg = self.out_queues[dst].get(timeout=0.3)
            except queue.Empty:
                continue
            if sock is None:
                sock = self._connect(dst)
                if sock is None:
                    return
            if delay > 0:
                time.sleep(delay)
            try:
                sock.sendall(message.encode(msg))
            except OSError:
                sock = self._connect(dst)
                if sock is None:
                    return
                try:
                    sock.sendall(message.encode(msg))
                except OSError:
                    return

    # ---------- shutdown ----------

    def stop(self):
        self.running = False
        try:
            self.listener.close()
        except OSError:
            pass
