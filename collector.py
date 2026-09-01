"""
collector.py
------------
Run after all four processes have finished.

    python3 collector.py [output_dir]

Does three things:

  1. Prints the recorded global state: every process's local state plus the
     contents of all 12 directed communication channels.

  2. Scans every pair of events across all processes with the vector clock
     comparison rule and reports concurrent pairs.

  3. Verifies that the recorded cut is consistent, using two independent checks:

     Orphan check      every message recorded as received inside the cut must
                       also be recorded as sent inside the cut. An orphan means
                       an inconsistent (impossible) global state.

     Conservation      sent_inside_cut minus received_inside_cut must equal
                       exactly the set of messages recorded as in flight in the
                       channel states. This proves nothing was lost or invented.
"""

import json
import os
import sys

import vector_clock


def load(out_dir):
    snaps, events = {}, {}
    for name in sorted(os.listdir(out_dir)):
        path = os.path.join(out_dir, name)
        if name.startswith("snapshot_P") and name.endswith(".json"):
            with open(path) as f:
                d = json.load(f)
            snaps[d["process_id"]] = d
        elif name.startswith("events_P") and name.endswith(".json"):
            with open(path) as f:
                d = json.load(f)
            events[d["process_id"]] = d
    return snaps, events


def rule(char="-", width=78):
    print(char * width)


def show_global_state(snaps):
    rule("=")
    print("RECORDED GLOBAL STATE (Chandy-Lamport snapshot)")
    rule("=")

    for pid in sorted(snaps):
        s = snaps[pid]
        st = dict(s["local_state"])
        sent = st.pop("sent_ids", [])
        recv = st.pop("received_ids", [])
        tag = "  [initiator]" if s.get("initiator") else ""
        print("\nP%d %s%s" % (pid, s["process_name"], tag))
        print("   vector clock at cut : %s" % vector_clock.fmt(s["vector_clock_at_snapshot"]))
        print("   application state   : %s" % json.dumps(st, sort_keys=True))
        print("   messages sent       : %s" % (sent if sent else "none"))
        print("   messages received   : %s" % (recv if recv else "none"))

    print("\nCHANNEL STATES (in-flight messages captured by the cut)")
    rule()
    any_inflight = False
    for pid in sorted(snaps):
        for chan, msgs in sorted(snaps[pid]["incoming_channels"].items()):
            if msgs:
                any_inflight = True
                for m in msgs:
                    print("   %-10s %s %s %s" % (chan, m["msg_id"], m["type"],
                                                 vector_clock.fmt(m["vc"])))
            else:
                print("   %-10s empty" % chan)
    if not any_inflight:
        print("\n   note: no message was in flight when the cut was taken")


def show_concurrency(events):
    rule("=")
    print("CAUSALITY ANALYSIS (vector clock comparison)")
    rule("=")

    all_events = []
    for pid in sorted(events):
        all_events.extend(events[pid]["events"])

    pairs = []
    for i in range(len(all_events)):
        for j in range(i + 1, len(all_events)):
            a, b = all_events[i], all_events[j]
            if a["process_id"] == b["process_id"]:
                continue  # events on one process are always causally ordered
            if vector_clock.compare(a["vc"], b["vc"]) == vector_clock.CONCURRENT:
                pairs.append((a, b))

    print("\ntotal events across all processes : %d" % len(all_events))
    print("concurrent pairs detected         : %d" % len(pairs))

    if pairs:
        print("\nsample of concurrent pairs (neither vector dominates the other):")
        for a, b in pairs[:8]:
            print("   P%d %-18s %-14s  ||  P%d %-18s %s" % (
                a["process_id"], a["label"], vector_clock.fmt(a["vc"]),
                b["process_id"], b["label"], vector_clock.fmt(b["vc"])))

    # a demonstrated causal pair, for contrast
    print("\nfor contrast, a causally ordered pair:")
    for a in all_events:
        for b in all_events:
            if a["process_id"] == b["process_id"]:
                continue
            if vector_clock.compare(a["vc"], b["vc"]) == vector_clock.BEFORE:
                print("   P%d %-18s %-14s  ->  P%d %-18s %s" % (
                    a["process_id"], a["label"], vector_clock.fmt(a["vc"]),
                    b["process_id"], b["label"], vector_clock.fmt(b["vc"])))
                return len(pairs)
    return len(pairs)


def check_consistency(snaps):
    rule("=")
    print("CONSISTENCY VERIFICATION")
    rule("=")

    sent, received = set(), set()
    owner = {}
    for pid, s in snaps.items():
        for m in s["local_state"].get("sent_ids", []):
            sent.add(m)
            owner[m] = pid
        for m in s["local_state"].get("received_ids", []):
            received.add(m)

    in_flight = set()
    for s in snaps.values():
        for msgs in s["incoming_channels"].values():
            for m in msgs:
                in_flight.add(m["msg_id"])

    orphans = received - sent
    expected_in_flight = sent - received

    print("\nmessages sent inside the cut      : %d %s" % (len(sent), sorted(sent)))
    print("messages received inside the cut  : %d %s" % (len(received), sorted(received)))
    print("messages recorded in channels     : %d %s" % (len(in_flight), sorted(in_flight)))

    print("\nCheck 1 - orphan messages (received but never sent)")
    if orphans:
        print("   FAIL: %s" % sorted(orphans))
    else:
        print("   PASS: none. Every recorded receive has a matching recorded send.")

    print("\nCheck 2 - conservation (sent - received must equal channel contents)")
    if expected_in_flight == in_flight:
        print("   PASS: %s" % (sorted(in_flight) if in_flight else "both empty"))
    else:
        print("   FAIL: expected %s but channels hold %s"
              % (sorted(expected_in_flight), sorted(in_flight)))

    ok = (not orphans) and (expected_in_flight == in_flight)

    rule()
    print("VERDICT: the recorded global state is %s" % ("CONSISTENT" if ok else "INCONSISTENT"))
    rule()
    if ok:
        print(
            "\nWhy: the cut contains no orphan message, so it corresponds to a state\n"
            "the system could genuinely have passed through. Messages that were sent\n"
            "but not yet delivered are not errors: they are correctly captured as\n"
            "channel state. FIFO channels guarantee a marker cannot overtake an\n"
            "earlier application message, which is what rules orphans out."
        )
    return ok


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "./out"
    snaps, events = load(out_dir)

    if not snaps:
        print("no snapshot files found in %s" % out_dir)
        sys.exit(1)
    if len(snaps) != len(events):
        print("warning: %d snapshots but %d event logs" % (len(snaps), len(events)))

    show_global_state(snaps)
    n_conc = show_concurrency(events)
    ok = check_consistency(snaps)

    report = {
        "processes": len(snaps),
        "concurrent_pairs": n_conc,
        "consistent": ok,
    }
    with open(os.path.join(out_dir, "collector_report.json"), "w") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
