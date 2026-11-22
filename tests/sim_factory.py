"""TEST-ONLY broker factory for the engine *process* (enabled by RB_TEST_BROKER_FACTORY).

A real-time SimBroker whose account state is persisted to a JSON file (like a broker
server that survives terminal restarts), and which starts a fake 'terminal' process on
connect (like mt5.initialize starting terminal64.exe). SYNTHETIC data only.
"""
from __future__ import annotations

import itertools
import json
import os
import random
import subprocess
import time

from tests.sim_broker import SimBroker


def _ticks(start: float, seconds: float, seed: int = 9):
    rnd = random.Random(seed)
    t, mid, out = start, 2650.0, []
    while t < start + seconds:
        t += 0.25
        mid += rnd.gauss(0, 0.03)
        out.append((round(t, 3), round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return out


class RealtimeSim(SimBroker):
    def __init__(self):
        base = float(int(time.time()) - 300)
        super().__init__({"XAUUSD": _ticks(base, 4 * 3600)})
        self.state_path = os.environ["RB_SIM_STATE"]
        self.term_path = os.environ.get("RB_SIM_TERMINAL", "")
        self._load()

    # persistence (the "broker server")
    def _load(self):
        if not os.path.exists(self.state_path):
            return
        with open(self.state_path) as fh:
            st = json.load(fh)
        self.balance = st.get("balance", self.balance)
        self.positions_ = {int(k): v for k, v in st.get("positions", {}).items()}
        self.orders_ = {int(k): v for k, v in st.get("orders", {}).items()}
        self.deals_ = st.get("deals", [])
        self.sent_orders = st.get("sent_orders", 0)
        self.tickets = itertools.count(st.get("next_ticket", 5000))

    def _save(self):
        nxt = next(self.tickets)
        self.tickets = itertools.count(nxt)
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"balance": self.balance, "positions": self.positions_, "orders": self.orders_,
                       "deals": self.deals_, "sent_orders": self.sent_orders, "next_ticket": nxt}, fh)
        os.replace(tmp, self.state_path)

    def _now(self):
        self.advance(time.time())

    def connect(self, login, password, server, timeout_ms=60000):
        self._now()
        if self.term_path and not getattr(self, "_term", None):
            # like mt5.initialize(): starts a separate terminal process that outlives shutdown()
            self._term = subprocess.Popen([self.term_path, "3600"], start_new_session=True,
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return super().connect(login, password, server, timeout_ms)

    def shutdown(self):
        super().shutdown()
        self._save()

    def _close(self, ticket, price, magic):
        super()._close(ticket, price, magic)
        self._save()


def _wrap(name):
    orig = getattr(SimBroker, name)

    def f(self, *a, **k):
        self._now()
        r = orig(self, *a, **k)
        if name in ("market_order", "cancel_order", "modify_sl"):
            self._save()
        return r
    return f


for _n in ("is_connected", "terminal", "account", "ticks_since", "last_tick", "positions", "position", "orders",
           "deals", "market_order", "close_position", "cancel_order", "modify_sl", "symbol_info"):
    setattr(RealtimeSim, _n, _wrap(_n))


def make():
    return RealtimeSim()


def seed_state(path: str, magic: int):
    """Pre-existing account state: one bot position, one manual position, one bot pending, one foreign pending."""
    now = time.time()
    st = {"balance": 10000.0, "sent_orders": 0, "next_ticket": 7000, "deals": [
        {"ticket": 6999, "position_id": 7001, "time": now - 30, "magic": magic, "entry": 0, "profit": 0.0,
         "commission": 0.0, "swap": 0.0, "fee": 0.0, "symbol": "XAUUSD"}],
        "positions": {
            "7001": {"ticket": 7001, "symbol": "XAUUSD", "type": 0, "volume": 0.05, "price_open": 2650.0,
                     "price_current": 2650.0, "sl": 2645.0, "tp": 2660.0, "profit": 0.0, "swap": 0.0,
                     "magic": magic, "comment": "RBurst-seed", "time": now - 30},
            "7002": {"ticket": 7002, "symbol": "XAUUSD", "type": 1, "volume": 0.10, "price_open": 2650.0,
                     "price_current": 2650.0, "sl": 0.0, "tp": 0.0, "profit": 0.0, "swap": 0.0,
                     "magic": 0, "comment": "manual", "time": now - 30}},
        "orders": {"7003": {"ticket": 7003, "symbol": "XAUUSD", "magic": magic, "type": 2},
                   "7004": {"ticket": 7004, "symbol": "XAUUSD", "magic": 555, "type": 2}}}
    with open(path, "w") as fh:
        json.dump(st, fh)
