# Distributed System Monitor — Food Delivery

Distributed Computing (CCZG 526), Lab Assignment I.
Python 3, standard library only. No installation required.

Vector clocks for event ordering and concurrency detection, plus the
Chandy–Lamport algorithm for recording a consistent global snapshot.

---

## Files

| File | Purpose |
|---|---|
| `config.json` | Single-machine config (development) |
| `config_nodes.json` | Three-VM lab config (demo) |
| `config_quiet_snapshot.json` | Variant used for the empty-channel test case |
| `vector_clock.py` | Three clock rules, causal comparison |
| `message.py` | Wire format, message ids |
| `network.py` | TCP layer, per-channel FIFO queues, link delay |
| `process.py` | Base process: events, state, dispatch loop |
| `snapshot.py` | Chandy–Lamport marker logic |
| `roles.py` | The four role behaviours |
| `main.py` | Entry point |
| `collector.py` | Merges snapshots, finds concurrency, verifies consistency |
| `run_local.sh` | Launches all four processes on one machine |

---

## The system

| Idx | Role | Node | Port |
|---|---|---|---|
| P0 | Order service (snapshot initiator) | node1 | 5000 |
| P1 | Payment service | node1 | 5001 |
| P2 | Restaurant | node2 | 5002 |
| P3 | Delivery partner | node3 | 5003 |

Message flow:

```
P0 --PAY_REQUEST--> P1 --PAY_OK--> P0 --ORDER_DISPATCH--> P2
   --FOOD_READY--> P3 --DELIVERED--> P0
```

Internal events: `ORDER_RECEIVED`, `ORDER_CLOSED` (P0), `PAYMENT_VALIDATED` (P1),
`COOKING_DONE` (P2), `GPS_HEARTBEAT`, `PICKED_UP` (P3).

The topology is treated as fully connected — 12 directed channels. Application
messages use only a few of them, but markers travel on all of them, which makes
the snapshot correct for any message pattern rather than just this scenario.

---

## Running on one machine

```bash
bash run_local.sh
```

This starts P1, P2, P3, then P0 (which drives the scenario), waits for the run to
finish, prints every process log, then runs the collector.

For the empty-channel variant:

```bash
bash run_local.sh config_quiet_snapshot.json
```

## Running on the three lab VMs

**1. Open the ports on every node** (Rocky 9.5 blocks them by default; a blocked
port looks exactly like a bug in the code):

```bash
sudo firewall-cmd --add-port=5000-5003/tcp --permanent
sudo firewall-cmd --reload
```

**2. Copy the project to all three nodes** and edit `config_nodes.json` with the
real IPs. The file must be identical on every node.

**3. Start the processes.** P0 waits `start_delay` seconds before beginning, so
start it last:

```bash
# node2
python3 main.py 2 config_nodes.json | tee log_P2.txt
# node3
python3 main.py 3 config_nodes.json | tee log_P3.txt
# node1
python3 main.py 1 config_nodes.json | tee log_P1.txt
python3 main.py 0 config_nodes.json | tee log_P0.txt
```

Connections retry until the peer is up, so any start order works. `start_delay`
just guarantees the scenario does not begin before everyone is listening.

**4. Collect the results.** Copy every `out/snapshot_P*.json` and
`out/events_P*.json` onto one node, then:

```bash
python3 collector.py ./out
```

---

## Test cases

| # | Test | What it shows | How to run |
|---|---|---|---|
| 1 | Normal order lifecycle | All 5 application messages delivered; vector clocks increase monotonically along the causal chain | `bash run_local.sh` |
| 2 | Concurrency detection | Collector reports `GPS_HEARTBEAT (0,0,0,1)` concurrent with `COOKING_DONE (4,3,2,0)` — neither vector dominates | collector output, causality section |
| 3 | Causal ordering | `ORDER_RECEIVED (1,0,0,0)` correctly reported as happening before `PAY_REQUEST <- P0 (2,1,0,0)` on a different process, with no clock synchronisation | collector output, contrast pair |
| 4 | Snapshot with empty channels | Snapshot taken when the system is quiet; all 12 channels empty, sent equals received | `bash run_local.sh config_quiet_snapshot.json` |
| 5 | Snapshot with an in-flight message | Snapshot taken mid-traffic; channel `P2->P3` holds `FOOD_READY`, and the totals only balance when it is counted | `bash run_local.sh` |
| 6 | Consistency verification | Zero orphan messages, conservation holds, verdict `CONSISTENT` | collector output, verification section |
| 7 | Start-order independence | Start P3 last; system still converges, proving no synchronised start or global clock is assumed | start processes in any order |

---

## Interpreting the captured global state

A representative run records:

```
P0 order_service    orders_placed=1  orders_completed=0   sent P0-1,P0-2  recv P1-1
P1 payment_service  amount_collected=450                  sent P1-1       recv P0-1
P2 restaurant       orders_cooked=1  orders_in_kitchen=0  sent P2-1       recv P0-2
P3 delivery_partner deliveries_done=0                     sent none       recv none

channel P2->P3      P2-1 FOOD_READY (4,3,3,0)
all other channels  empty
```

Read as a moment in the business process: payment has been collected, the
restaurant has finished cooking, and the food-ready notification is on its way to
a delivery partner who has not yet heard about it. No single process knows this,
but the snapshot does.

Note that the process states alone look unbalanced — the restaurant has sent
something nobody has received. The in-flight message on `P2->P3` is exactly what
accounts for the difference. This is why channel state is part of a global state
and not an optional extra.

### Is it consistent?

Yes, and the collector proves it two ways.

**Orphan check.** Every `msg_id` recorded as received appears in some process's
recorded sent list. There is no receive without a matching send, so the cut
corresponds to a state the system could genuinely have passed through.

**Conservation.** Messages sent inside the cut minus messages received inside the
cut equals exactly the set recorded in the channels. Nothing was lost or
invented.

The underlying guarantee comes from FIFO channels. A marker cannot overtake an
application message sent earlier on the same channel, so anything arriving after
a marker must have been sent after the sender had already snapshotted, and
therefore correctly falls outside the cut.

---

## Features beyond the problem statement

- Automated concurrency detector comparing every cross-process event pair, rather
  than one hand-picked example.
- Automated consistency verifier with two independent checks (orphan detection
  and message conservation).
- Configurable per-channel link delay, which makes the in-flight snapshot case
  deterministic instead of a timing race, while preserving FIFO ordering.
- Config-driven deployment: identical source runs on one machine or three nodes.
- Start-order independence via connection retry.
- Fully connected marker topology, so the snapshot is correct for any
  communication pattern.
- Machine-readable JSON output (`snapshot_P*.json`, `events_P*.json`,
  `collector_report.json`) alongside human-readable logs.
