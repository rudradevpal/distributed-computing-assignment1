"""
snapshot.py
-----------
Chandy-Lamport global snapshot algorithm.

Assumptions: channels are FIFO (guaranteed by network.py) and reliable.

Initiator:
    record own local state
    start recording on every incoming channel
    send a MARKER on every outgoing channel

On receiving MARKER on channel c:
    if this is the FIRST marker this process has seen:
        record own local state
        record the state of channel c as EMPTY
        start recording on every OTHER incoming channel
        send a MARKER on every outgoing channel
    else:
        stop recording on channel c
        whatever was logged on c since recording began IS the state of c

Termination for this process: a marker has arrived on all N-1 incoming channels.

Why it is correct: because channels are FIFO, any application message that
arrives on channel c after the marker must have been sent after the sender had
already taken its own snapshot. So it correctly falls outside the cut, and no
"orphan" (received-but-never-sent) message can appear in the recording.
"""

import copy
import json
import os

import message


class SnapshotManager:
    def __init__(self, proc):
        self.proc = proc
        self.n = proc.n
        self.idx = proc.idx

        self.taken = False              # has this process recorded its local state
        self.local_state = None
        self.local_vc = None
        self.initiator = False

        self.peers = [p["id"] for p in proc.cfg["processes"] if p["id"] != self.idx]

        self.recording_on = {p: False for p in self.peers}   # incoming channel -> recording?
        self.channel_state = {p: [] for p in self.peers}     # incoming channel -> in-flight msgs
        self.markers_from = set()

        self.completed = False

    # ---------- taking the local state ----------

    def _record_local_state(self, reason):
        self.taken = True
        self.local_state = copy.deepcopy(self.proc.state)
        self.local_vc = self.proc.vc.copy()
        self.proc.log("SNAPSHOT local state recorded (%s) state=%s vc=%s"
                      % (reason, json.dumps(self.local_state, sort_keys=True),
                         self.proc.vc))

    def _flood_markers(self):
        for dst in self.peers:
            m = message.make(message.MARKER, self.idx, dst, self.proc.vc.copy(),
                             {"snapshot_id": "S1"})
            self.proc.net.send(m)
        self.proc.log("SNAPSHOT markers sent on all outgoing channels %s" % (self.peers,))

    # ---------- entry points ----------

    def initiate(self):
        if self.taken:
            return
        self.initiator = True
        self._record_local_state("initiator")
        for p in self.peers:
            self.recording_on[p] = True
            self.channel_state[p] = []
        self._flood_markers()

    def on_marker(self, msg):
        src = msg["src"]

        if not self.taken:
            self._record_local_state("first marker, from P%d" % src)
            # the channel the marker arrived on is, by definition, empty
            self.recording_on[src] = False
            self.channel_state[src] = []
            for p in self.peers:
                if p != src:
                    self.recording_on[p] = True
                    self.channel_state[p] = []
            self._flood_markers()
        else:
            # already recording: this marker closes channel src
            self.recording_on[src] = False
            self.proc.log("SNAPSHOT channel P%d->P%d closed with %d in-flight message(s)"
                          % (src, self.idx, len(self.channel_state[src])))

        self.markers_from.add(src)
        if len(self.markers_from) == len(self.peers):
            self._finalise()

    def on_app_message(self, msg):
        """Called for every application message BEFORE it is delivered to the app."""
        src = msg["src"]
        if self.taken and self.recording_on.get(src):
            self.channel_state[src].append(message.summary(msg))
            self.proc.log("SNAPSHOT recorded in-flight %s on channel P%d->P%d"
                          % (msg["msg_id"], src, self.idx))

    # ---------- output ----------

    def _finalise(self):
        self.completed = True
        out_dir = self.proc.out_dir
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "snapshot_P%d.json" % self.idx)

        data = {
            "process_id": self.idx,
            "process_name": self.proc.name,
            "initiator": self.initiator,
            "local_state": self.local_state,
            "vector_clock_at_snapshot": self.local_vc,
            "incoming_channels": {
                "P%d->P%d" % (src, self.idx): self.channel_state[src]
                for src in self.peers
            },
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)

        self.proc.log("SNAPSHOT complete, written to %s" % path)
