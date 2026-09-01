"""
message.py
----------
Wire format: one JSON object per line (newline-delimited JSON) over TCP.

Every message carries:
  msg_id   globally unique, "P<src>-<n>". The consistency checker depends on this.
  type     application type or MARKER
  src,dst  process indices
  vc       the sender's vector timestamp at the moment of sending
  payload  application data
"""

import json
import threading

# Application message types
PAY_REQUEST = "PAY_REQUEST"
PAY_OK = "PAY_OK"
ORDER_DISPATCH = "ORDER_DISPATCH"
FOOD_READY = "FOOD_READY"
DELIVERED = "DELIVERED"

# Control message type used by the snapshot algorithm
MARKER = "MARKER"

APP_TYPES = {PAY_REQUEST, PAY_OK, ORDER_DISPATCH, FOOD_READY, DELIVERED}

_seq_lock = threading.Lock()
_seq = {}


def next_msg_id(src):
    """Monotonic per-process message id, e.g. P0-1, P0-2, ..."""
    with _seq_lock:
        _seq[src] = _seq.get(src, 0) + 1
        return "P%d-%d" % (src, _seq[src])


def make(mtype, src, dst, vc, payload=None):
    return {
        "msg_id": next_msg_id(src),
        "type": mtype,
        "src": src,
        "dst": dst,
        "vc": list(vc),
        "payload": payload if payload is not None else {},
    }


def is_marker(msg):
    return msg["type"] == MARKER


def encode(msg):
    return (json.dumps(msg) + "\n").encode("utf-8")


def decode(line):
    return json.loads(line.decode("utf-8"))


def summary(msg):
    """Compact form stored in channel state and printed in logs."""
    return {
        "msg_id": msg["msg_id"],
        "type": msg["type"],
        "src": msg["src"],
        "dst": msg["dst"],
        "vc": msg["vc"],
        "payload": msg["payload"],
    }
