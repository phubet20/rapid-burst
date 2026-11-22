"""Basket mode: averaging + pyramiding + compounding, margin-use cap (70% of equity), basket exits
(profit target with trailed profit, moderate basket loss stop). SimBroker + SYNTHETIC ticks (test double)."""
from rapidburst.config import EngineConfig
from rapidburst.engine import PositionState
from tests.harness import make, now_base, run
from tests.synth import burst_series
from tests.test_burst import _small_account, _start

T0 = now_base()


def _path(up_after: float = 40.0):
    """impulse up, then a slow grind DOWN (against the basket -> averaging), then a strong rally."""
    ticks = burst_series(T0, +1, pre_seconds=150, burst_seconds=8, step=0.08, seed=3)
    t, mid = ticks[-1][0], (ticks[-1][1] + ticks[-1][2]) / 2
    for i in range(1, 1500):
        t += 0.3
        mid += -0.05 if i < 700 else (+0.12 if i < 1300 else 0.0)
        ticks.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return ticks


def _cfg(**kw):
    kw.setdefault("basket_enabled", True)
    return EngineConfig(**kw)


def test_basket_averages_stays_inside_margin_cap_and_closes_together(tmp_path):
    cfg = _cfg(max_margin_use_pct=70.0, basket_avg_atr=1.0, basket_add_cooldown=2.0, basket_max_orders=6)
    tr, br = make(tmp_path, {"BTCUSDm": _path()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=800.0)       # 0.01 lot = 8 margin; cap 700 -> plenty, ~87 lots
    peak = {"use": 0.0, "n": 0}

    def watch(t):
        eq = br.account()["equity"]
        used = sum(p["volume"] * 800.0 for p in br.positions_.values())
        peak["use"] = max(peak["use"], used / eq * 100 if eq else 0)
        peak["n"] = max(peak["n"], len(br.positions_))
        for p in br.positions_.values():
            assert p["tp"] == 0.0 and p["sl"] > 0          # no TP, a (far) broker SL
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 700, on_step=watch)
    msgs = [l["msg"] for l in tr.logs]
    assert any(m.startswith("BASKET OPEN") for m in msgs), msgs[-10:]
    assert any(m.startswith("BASKET AVG") for m in msgs), msgs[-10:]          # adds against the position
    assert peak["n"] >= 2 and peak["n"] <= cfg.basket_max_orders
    assert peak["use"] <= cfg.max_margin_use_pct + 1e-6
    # no tight-SL / wrong-way exits exist any more in basket mode
    assert not any("wrong-way" in m.lower() or "BE+" in m for m in msgs)
    # basket view for the UI
    snap_items = tr._basket_view()["items"]
    assert snap_items and all({"net", "target", "loss_limit", "orders"} <= set(v) for v in snap_items.values())
    run(tr, br, T0 + 700, T0 + 1500, on_step=watch)
    exits = [m for m in [l["msg"] for l in tr.logs] if m.startswith("EXIT BATCH")]
    assert exits and all("basket" in m for m in exits), exits
    assert "profit trail" in " ".join(exits)                          # closed on the (trailed) basket profit
    closed = [m for m in [l["msg"] for l in tr.logs] if "fully closed" in m]
    assert closed and "P/L +" in closed[0]
    assert peak["use"] <= cfg.max_margin_use_pct + 1e-6


def test_margin_cap_blocks_adds_and_open(tmp_path):
    import re
    cfg = _cfg(max_margin_use_pct=70.0, basket_max_orders=8)
    tr, br = make(tmp_path, {"BTCUSDm": _path()}, cfg)
    # 0.01 lot = 40 margin on a $100 account: cap = 70% of equity. Every order must fit in it.
    _small_account(br, balance=100.0, margin_per_lot=4000.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 700)
    used = [(float(m.group(1)), float(m.group(2))) for l in tr.logs
            for m in [re.search(r"margin used ([\d.]+)/([\d.]+) \(70% of equity", l["msg"])] if m]
    assert used and all(u <= cap + 1e-6 for u, cap in used), used
    assert tr.skip_counts["BTCUSDm"]["margin"] > 0, dict(tr.skip_counts)
    # first order of the first basket: 40 margin fits, a 2nd (80) does not while equity is ~100
    assert used[0][0] == 40.0 and used[0][1] <= 100.0


def _seed_basket(tr, br, profit_per_leg, n=3, eq0=1000.0):
    tr.broker.connect(1, "x", "y")
    for i in range(n):
        tr.pos_state[100 + i] = PositionState(ticket=100 + i, symbol="BTCUSDm", side="BUY", volume=0.1,
                                              open_price=100.0, open_time=0.0, sl=50.0, tp=0.0, batch_id="bk1")
    tr.bursts["bk1"] = {"symbol": "BTCUSDm", "side": "BUY", "basket": True, "eq0": eq0, "peak": 0.0, "armed": False,
                        "realized": 0.0, "legs": n}
    tr.account = {"equity": eq0, "balance": eq0}
    return {100 + i: {"ticket": 100 + i, "profit": profit_per_leg, "swap": 0.0} for i in range(n)}


def test_basket_loss_stop_is_30pct_of_equity_not_70(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": _path()}, _cfg())
    assert tr.cfg.basket_loss_pct == 30.0 and tr.cfg.max_margin_use_pct == 70.0
    live = _seed_basket(tr, br, -90.0)               # -270 net = 27% of 1000: still held (no wrong-way cut)
    legs = [tr.pos_state[t] for t in live]
    assert tr._basket_exit("bk1", legs, live) is None
    live = {t: dict(p, profit=-101.0) for t, p in live.items()}     # -303 = 30.3%
    why = tr._basket_exit("bk1", legs, live)
    assert why and "basket loss stop" in why and "30%" in why


def test_basket_profit_target_then_trailing(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": _path()}, _cfg(basket_target_pct=2.0, basket_trail_frac=0.3))
    live = _seed_basket(tr, br, 5.0)                  # +15 < target 20: keep holding
    legs = [tr.pos_state[t] for t in live]
    assert tr._basket_exit("bk1", legs, live) is None and not tr.bursts["bk1"]["armed"]
    live = {t: dict(p, profit=10.0) for t, p in live.items()}      # +30 >= 20: armed, trailing, not closed yet
    assert tr._basket_exit("bk1", legs, live) is None and tr.bursts["bk1"]["armed"]
    live = {t: dict(p, profit=15.0) for t, p in live.items()}      # +45 new peak
    assert tr._basket_exit("bk1", legs, live) is None
    live = {t: dict(p, profit=12.0) for t, p in live.items()}      # +36 > 45*0.7=31.5: hold
    assert tr._basket_exit("bk1", legs, live) is None
    live = {t: dict(p, profit=10.0) for t, p in live.items()}      # +30 <= 31.5: close the whole basket
    why = tr._basket_exit("bk1", legs, live)
    assert why and "profit trail" in why and "peak +45.00" in why
    m = tr._basket_metrics("bk1", legs, live)
    assert m["trail_floor"] == 31.5 and m["armed"]


def test_config_env_overrides(monkeypatch):
    monkeypatch.setenv("RB_MAX_MARGIN_USE_PCT", "55")
    monkeypatch.setenv("RB_BASKET_MAX_LOSS_PCT", "20")
    c = EngineConfig.load()
    assert c.max_margin_use_pct == 55.0 and c.basket_loss_pct == 20.0
