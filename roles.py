"""
roles.py
--------
The four processes of the food delivery system.

Causal chain:
    P0 --PAY_REQUEST--> P1 --PAY_OK--> P0 --ORDER_DISPATCH--> P2
       --FOOD_READY--> P3 --DELIVERED--> P0

Internal events: ORDER_RECEIVED and ORDER_CLOSED (P0), PAYMENT_VALIDATED (P1),
COOKING_DONE (P2), GPS_HEARTBEAT and PICKED_UP (P3).

The GPS_HEARTBEAT on P3 fires before P3 has communicated with anyone, so it is
causally unrelated to everything happening on the order side. That is the
guaranteed concurrent pair the assignment asks for.
"""

import threading
import time

import message
from process import Process


class OrderService(Process):
    """P0 - central order processing, and the snapshot initiator."""

    def setup(self):
        self.state = {
            "orders_placed": 0,
            "orders_completed": 0,
            "sent_ids": [],
            "received_ids": [],
        }

    def on_start(self):
        # give every peer time to bind its listening socket
        time.sleep(float(self.cfg.get("start_delay", 3.0)))

        def place(st):
            st["orders_placed"] += 1

        self.internal("ORDER_RECEIVED", mutate=place)
        self.send(1, message.PAY_REQUEST, {"order_id": 101, "amount": 450})

    def on_message(self, msg):
        if msg["type"] == message.PAY_OK:
            self.send(2, message.ORDER_DISPATCH, {"order_id": 101, "items": ["biryani"]})

            # Fire the snapshot while application traffic is still moving, so a
            # message is genuinely in flight when the cut is taken.
            delay = float(self.cfg.get("snapshot_delay_after_dispatch", 1.5))
            threading.Timer(delay, self._start_snapshot).start()

        elif msg["type"] == message.DELIVERED:
            def close(st):
                st["orders_completed"] += 1

            self.internal("ORDER_CLOSED", mutate=close)

    def _start_snapshot(self):
        with self.lock:
            self.log("SNAPSHOT initiating global snapshot")
            self.snap.initiate()


class PaymentService(Process):
    """P1 - validates and collects payment."""

    def setup(self):
        self.state = {
            "amount_collected": 0,
            "sent_ids": [],
            "received_ids": [],
        }

    def on_message(self, msg):
        if msg["type"] == message.PAY_REQUEST:
            amount = msg["payload"].get("amount", 0)

            def collect(st):
                st["amount_collected"] += amount

            self.internal("PAYMENT_VALIDATED", mutate=collect)
            self.send(0, message.PAY_OK, {"order_id": msg["payload"].get("order_id")})


class Restaurant(Process):
    """P2 - accepts the order, cooks, then tells the delivery partner."""

    def setup(self):
        self.state = {
            "orders_in_kitchen": 0,
            "orders_cooked": 0,
            "sent_ids": [],
            "received_ids": [],
        }

    def on_message(self, msg):
        if msg["type"] == message.ORDER_DISPATCH:
            with self.lock:
                self.state["orders_in_kitchen"] += 1
            # cook without blocking the dispatch loop
            threading.Timer(0.5, self._finish_cooking, args=(msg,)).start()

    def _finish_cooking(self, msg):
        def cooked(st):
            st["orders_in_kitchen"] -= 1
            st["orders_cooked"] += 1

        self.internal("COOKING_DONE", mutate=cooked)
        self.send(3, message.FOOD_READY, {"order_id": msg["payload"].get("order_id")})


class DeliveryPartner(Process):
    """P3 - idles, then picks up and delivers."""

    def setup(self):
        self.state = {
            "deliveries_done": 0,
            "sent_ids": [],
            "received_ids": [],
        }

    def on_start(self):
        # An early local event with no causal link to the order flow.
        time.sleep(0.5)
        self.internal("GPS_HEARTBEAT")

    def on_message(self, msg):
        if msg["type"] == message.FOOD_READY:
            self.internal("PICKED_UP")

            def delivered(st):
                st["deliveries_done"] += 1

            self.send(0, message.DELIVERED,
                      {"order_id": msg["payload"].get("order_id")},
                      mutate=delivered)


ROLES = {
    0: OrderService,
    1: PaymentService,
    2: Restaurant,
    3: DeliveryPartner,
}


def build(cfg, idx):
    proc = ROLES[idx](cfg, idx)
    proc.setup()
    return proc
