"""Replay harness: drives the real Trader loop against SimBroker on SYNTHETIC ticks.
Test-only. Results are not live-market or MT5 verification."""
from __future__ import annotations

import time

from rapidburst.config import EngineConfig
from rapidburst.secure_store import SecureStore
from rapidburst.trader import Trader
from tests.sim_broker import SimBroker

# Pre-t30u risk limits. The engine defaults are now AGGRESSIVE (owner request); mechanism tests (BE/trail,
# protection, exits, adds) run with these moderate limits unless a test passes its own config.
CONSERVATIVE = dict(risk_per_trade_pct=0.5, risk_per_batch_pct=1.0, small_account_batch_risk_pct=10.0,
                    max_total_risk_pct=3.0, max_margin_use_pct=50.0, margin_use_max_pct=50.0,
                    min_margin_level_pct=200.0, max_total_lots=2.0, max_lot_per_order=1.0, max_batch_lots=1.0,
                    max_total_notional_x=500.0, max_positions_total=10, group_loss_pct=2.0,
                    equity_drawdown_pct=8.0, safe_dd_cap_pct=30.0, safe_probe_batches=3,
                    # t37u/t38u owner-mode features off for the mechanism tests (stop-based sizing + broker SL);
                    # tests/test_t38u.py covers the no-stop full-margin mode
                    no_stop_mode=False, full_batch=False, max_orders_per_batch=8, max_positions_per_symbol=8,
                    zones_enabled=False, pullback_hold=False, zone_target_exit=False, learning_enabled=True,
                    regime_enabled=False, hedge_enabled=False, avg_enabled=False, precision_enabled=False, m1_only=False, ladder_enabled=False, add_enabled=True)


# t37u/t38u owner-mode features OFF (stop-based sizing, broker SL, <=8 orders) for the pre-t38u mechanism tests
LEGACY_OWNER_OFF = dict(no_stop_mode=False, full_batch=False, max_orders_per_batch=8, max_positions_per_symbol=8,
                        zones_enabled=False, pullback_hold=False, zone_target_exit=False, learning_enabled=True,
                    regime_enabled=False, hedge_enabled=False, avg_enabled=False, precision_enabled=False, m1_only=False, ladder_enabled=False, add_enabled=True)


def legacy(**kw) -> EngineConfig:
    return EngineConfig(**dict(LEGACY_OWNER_OFF, **kw))


def conservative(**kw) -> EngineConfig:
    return EngineConfig(**dict(CONSERVATIVE, **kw))


class Invariants:
    def __init__(self):
        self.max_pos = 0
        self.max_pos_symbol = 0
        self.max_lots = 0.0
        self.orders_without_sl = 0
        self.orders_with_broker_sl = 0
        self.adds_into_loser = 0
        self.opposite_while_open = 0
        self.entries = 0


def instrument(broker: SimBroker, cfg: EngineConfig, inv: Invariants):
    orig = broker.market_order

    def wrapped(s, side, volume, sl, tp, deviation, magic, comment):
        mine = [p for p in broker.positions_.values() if p["magic"] == magic and p["symbol"] == s]
        batch = comment.rsplit("-", 1)[0] + "-"          # legs of the same burst batch (one signal)
        if any(p["profit"] <= 0 and not str(p.get("comment", "")).startswith(batch) for p in mine):
            inv.adds_into_loser += 1
        if any((p["type"] == 0) != (side == "BUY") for p in mine):
            inv.opposite_while_open += 1
        if not sl:
            inv.orders_without_sl += 1
        else:
            inv.orders_with_broker_sl += 1
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
    cfg = cfg or conservative()
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
