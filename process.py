"""
process.py
----------
Base distributed process: owns the vector clock, the local application state,
the event log, and the dispatch loop.

Three event kinds are recorded, exactly as the assignment requires:
  INTERNAL  a purely local action
  SEND      a message leaves this process
  RECEIVE   a message arrives at this process
"""

import json
import os
import queue
import threading
import time

import message
import vector_clock
from network import Network
from snapshot import SnapshotManager

INTERNAL = "INTERNAL"
SEND = "SEND"
RECEIVE = "RECEIVE"


class Process:
    def __init__(self, cfg, idx):
        self.cfg = cfg
        self.idx = idx
        self.procs = cfg["processes"]
        self.n = len(self.procs)
        self.name = self.procs[idx]["name"]
        self.out_dir = cfg.get("output_dir", "./out")

        self.vc = vector_clock.VectorClock(self.n, idx)
        self.state = {}
        self.event_log = []
        self.seq = 0
        self.lock = threading.RLock()
        self.start_time = time.time()

        self.inbox = queue.Queue()
        self.net = Network(cfg, idx, self.inbox, self.log)
        self.snap = SnapshotManager(self)

        self.running = True

    # ---------- logging ----------

    def log(self, text):
        stamp = time.time() - self.start_time
        line = "[%6.2fs] P%d %-16s | %s" % (stamp, self.idx, self.name, text)
        print(line, flush=True)

    def _record_event(self, kind, label, vc_after, extra=None):
        self.seq += 1
        ev = {
            "process_id": self.idx,
            "process_name": self.name,
            "seq": self.seq,
            "kind": kind,
            "label": label,
            "vc": list(vc_after),
            "wall_offset": round(time.time() - self.start_time, 3),
        }
        if extra:
            ev.update(extra)
        self.event_log.append(ev)
        self.log("%-8s %-22s vc=%s" % (kind, label, vector_clock.fmt(vc_after)))
        return ev

    # ---------- the three event kinds ----------

    def internal(self, label, mutate=None):
        with self.lock:
            if mutate:
                mutate(self.state)
            vc_after = self.vc.tick()                      # rule R1
            self._record_event(INTERNAL, label, vc_after)

    def send(self, dst, mtype, payload=None, mutate=None):
        with self.lock:
            if mutate:
                mutate(self.state)
            vc_after = self.vc.tick()                      # rule R2
            msg = message.make(mtype, self.idx, dst, vc_after, payload)
            self.state.setdefault("sent_ids", []).append(msg["msg_id"])
            self._record_event(SEND, "%s -> P%d" % (mtype, dst), vc_after,
                               {"msg_id": msg["msg_id"], "peer": dst})
            self.net.send(msg)
            return msg

    def _receive(self, msg):
        with self.lock:
            # The snapshot layer must see the message before the application does,
            # so an in-flight message is recorded as channel state.
            self.snap.on_app_message(msg)

            self.vc.tick()                                 # rule R3, first half
            self.vc.merge(msg["vc"])                       # rule R3, second half
            vc_after = self.vc.copy()
            self.state.setdefault("received_ids", []).append(msg["msg_id"])
            self._record_event(RECEIVE, "%s <- P%d" % (msg["type"], msg["src"]),
                               vc_after,
                               {"msg_id": msg["msg_id"], "peer": msg["src"]})
        self.on_message(msg)

    # ---------- to be provided by roles.py ----------

    def on_start(self):
        pass

    def on_message(self, msg):
        pass

    # ---------- main loop ----------

    def run(self):
        os.makedirs(self.out_dir, exist_ok=True)
        self.net.start()

        threading.Thread(target=self._safe_on_start, daemon=True).start()

        deadline = time.time() + float(self.cfg.get("run_seconds", 16))
        while self.running and time.time() < deadline:
            try:
                msg = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue
            if message.is_marker(msg):
                with self.lock:
                    self.log("MARKER   received from P%d" % msg["src"])
                    self.snap.on_marker(msg)
            else:
                self._receive(msg)

        self._shutdown()

    def _safe_on_start(self):
        try:
            self.on_start()
        except Exception as exc:            # keep one bad role from killing the run
            self.log("on_start error: %r" % exc)

    def _shutdown(self):
        self.running = False
        path = os.path.join(self.out_dir, "events_P%d.json" % self.idx)
        with open(path, "w") as f:
            json.dump({
                "process_id": self.idx,
                "process_name": self.name,
                "final_state": self.state,
                "final_vc": self.vc.copy(),
                "events": self.event_log,
            }, f, indent=2)
        self.log("event log written to %s (%d events)" % (path, len(self.event_log)))
        if not self.snap.completed:
            self.log("WARNING snapshot did not complete on this process")
        self.net.stop()
