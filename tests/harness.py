"""Replay harness: drives the real Trader loop against SimBroker on SYNTHETIC ticks.
Test-only. Results are not live-market or MT5 verification."""
from __future__ import annotations

import time

from rapidburst.config import EngineConfig
from rapidburst.secure_store import SecureStore
from rapidburst.trader import Trader
from tests.sim_broker import SimBroker


class Invariants:
    def __init__(self):
        self.max_pos = 0
        self.max_pos_symbol = 0
        self.max_lots = 0.0
        self.orders_without_sl = 0
        self.adds_into_loser = 0
        self.opposite_while_open = 0
        self.entries = 0


def instrument(broker: SimBroker, cfg: EngineConfig, inv: Invariants):
    orig = broker.market_order

    def wrapped(s, side, volume, sl, tp, deviation, magic, comment):
        mine = [p for p in broker.positions_.values() if p["magic"] == magic and p["symbol"] == s]
        if any(p["profit"] <= 0 for p in mine):
            inv.adds_into_loser += 1
        if any((p["type"] == 0) != (side == "BUY") for p in mine):
            inv.opposite_while_open += 1
        if not sl:
            inv.orders_without_sl += 1
        r = orig(s, side, volume, sl, tp, deviation, magic, comment)
        if r.get("ok"):
            inv.entries += 1
        own = [p for p in broker.positions_.values() if p["magic"] == magic]
        inv.max_pos = max(inv.max_pos, len(own))
        inv.max_pos_symbol = max(inv.max_pos_symbol, sum(1 for p in own if p["symbol"] == s))
        inv.max_lots = max(inv.max_lots, sum(p["volume"] for p in own))
        return r

    broker.market_order = wrapped


def make(tmp_path, ticks_by_symbol, cfg=None, **broker_kw):
    cfg = cfg or EngineConfig()
    broker = SimBroker(ticks_by_symbol, **broker_kw)
    store = SecureStore(str(tmp_path))
    store.save_credentials({"login": 12345678, "password": "test-only", "server": "Sim-Demo"})
    trader = Trader(broker, store, cfg, str(tmp_path))
    trader.selected = list(ticks_by_symbol)
    return trader, broker


def run(trader: Trader, broker: SimBroker, t_from: float, t_to: float, dt: float = 0.3, on_step=None):
    t = t_from
    while t <= t_to:
        broker.advance(t)
        trader.step(t)
        if on_step:
            on_step(t)
        t += dt


def now_base() -> float:
    return float(int(time.time()) - 6 * 3600)
