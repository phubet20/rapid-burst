"""t40u owner mode: SL locked off, UP/DOWN/SIDEWAYS + phase, pullback-wick entries with anti-chase, close only in
profit, recovery hedge, owner-requested averaging layers, cycle take-profit. SYNTHETIC data (not live results)."""
from rapidburst import regime
from rapidburst.config import RISK_ENV, TUNABLE, EngineConfig
from tests.harness import make, run
from tests.paths import T0, _small_account, _start, series_up
from tests.test_t34u import _controlled


def _b(i, o, h, l, c):
    return {"time": i * 300, "open": o, "high": h, "low": l, "close": c}


def _zig(points, step=4):
    """M5 bars walking through the given swing points."""
    bars, i, px = [], 0, points[0]
    for tgt in points[1:]:
        for k in range(step):
            nxt = px + (tgt - px) / (step - k)
            bars.append(_b(i, px, max(px, nxt) + 0.1, min(px, nxt) - 0.1, nxt))
            px, i = nxt, i + 1
    return bars


def test_stop_loss_is_locked_off():
    c = EngineConfig()
    assert c.no_stop_mode and "no_stop_mode" not in TUNABLE and "broker_sl" not in TUNABLE
    assert all(k != "broker_sl" for _, k in RISK_ENV)


def test_regime_up_down_sideways_and_phase():
    up = regime.analyze(_zig([100, 104, 102, 107, 105, 110, 108.5]))
    assert up.state == "UP" and up.phase >= 2 and up.phase % 2 == 0 and up.leg == "pullback"
    dn = regime.analyze(_zig([110, 106, 108, 103, 105, 100, 101.5]))
    assert dn.state == "DOWN" and dn.phase >= 2
    side = regime.analyze(_zig([100, 103, 100, 103, 100, 103, 100]))
    assert side.state == "SIDEWAYS"
    brk = regime.analyze(_zig([100, 104, 102, 107, 105, 110, 103]))    # below the last higher low
    assert brk.state != "UP"
    assert "phase" in up.label() and up.brief()["state"] == "UP"


def _session(tmp_path, **kw):
    cfg = EngineConfig(add_enabled=False, **kw)
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, cfg)
    br.hedging = True
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    ctl = _controlled(tr)
    return tr, br, ctl


def test_close_only_in_profit(tmp_path):
    tr, br, ctl = _session(tmp_path, hedge_enabled=False, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    held = set(br.positions_)
    assert held and all(not p["sl"] for p in br.positions_.values())
    ctl["side"] = "NO TRADE"
    tr._micro_exit = lambda *a, **k: "momentum faded (test)"
    tr._net = lambda legs, live: -5.0                                 # red basket: never closed by soft exits
    run(tr, br, T0 + 3.3, T0 + 8)
    assert held <= set(br.positions_)
    tr._net = lambda legs, live: 3.0                                  # green: closes (after anti-jitter confirm)
    run(tr, br, T0 + 8.3, T0 + 11)
    assert not (held & set(br.positions_))


def test_recovery_hedge_opens_opposite_basket(tmp_path):
    # t44u: hedging is OFF by default and locked off in load(); this exercises the retained legacy path explicitly
    tr, br, ctl = _session(tmp_path, recovery_after_s=5.0, hedge_enabled=True, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    assert br.positions_ and {p["type"] for p in br.positions_.values()} == {0}
    ctl["side"] = "SELL"
    tr._micro_exit = lambda *a, **k: None
    tr._net = lambda legs, live: -1.0 if legs and legs[0].side == "BUY" else 0.0
    run(tr, br, T0 + 3.3, T0 + 10)
    sides = {p["type"] for p in br.positions_.values()}
    assert sides == {0, 1}                                            # BUY kept (no loss close) + SELL hedge
    assert tr.recoveries["hedges"] >= 1
    assert any(l["msg"].startswith("RECOVERY HEDGE") for l in tr.logs)


def test_recovery_layers_at_better_prices_capped(tmp_path):
    tr, br, ctl = _session(tmp_path, hedge_enabled=False, avg_enabled=True, avg_max_layers=2, avg_spacing_atr=0.5)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    assert br.positions_ and {p["type"] for p in br.positions_.values()} == {0}
    ctl["side"] = "NO TRADE"
    n0 = len(br.positions_)
    tr._micro_exit = lambda *a, **k: None
    tr._net = lambda legs, live: -2.0
    real = tr._micro
    off = {"x": 0.0}

    def lower(s, now_wall):
        m, st = real(s, now_wall)
        if m.ready:
            m.ask, m.bid = m.ask - off["x"], m.bid - off["x"]
        return m, st
    tr._micro = lower
    t = T0 + 3.3
    for _ in range(6):                                               # price keeps falling -> layers, then the cap
        off["x"] += 2.0 * max(tr._micro("BTCUSDm", t)[0].atr, 1e-6)
        run(tr, br, t, t + 1.2)
        t += 1.5
    assert tr.recoveries["layers"] == 2 and len(br.positions_) > n0
    adds = [p for p in br.positions_.values() if "-r" in p["comment"]]
    first = sum(p["volume"] for p in br.positions_.values() if "-r" not in p["comment"])
    for layer in ("-r1", "-r2"):                                    # avg_lot_mult 1.0: each layer = the batch's lots
        assert abs(sum(p["volume"] for p in adds if layer in p["comment"]) - first) < 1e-6
    assert sum("RECOVERY ADD" in l["msg"] and "owner-requested averaging" in l["msg"] for l in tr.logs) == 2


def test_cycle_take_profit_closes_hedged_legs_together(tmp_path):
    tr, br, ctl = _session(tmp_path, recovery_after_s=1.0, hedge_enabled=True, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    assert br.positions_ and {p["type"] for p in br.positions_.values()} == {0}
    ctl["side"] = "SELL"
    tr._micro_exit = lambda *a, **k: None
    tr._net = lambda legs, live: -1.0 if legs and all(ps.side == "BUY" for ps in legs) else 0.0
    run(tr, br, T0 + 3.3, T0 + 6)
    assert {p["type"] for p in br.positions_.values()} == {0, 1}
    ctl["side"] = "NO TRADE"
    tr._net = lambda legs, live: 50.0 if len({ps.side for ps in legs}) > 1 else -1.0
    run(tr, br, T0 + 6.3, T0 + 8)
    assert not br.positions_ and tr.recoveries["cycle_tp"] >= 1
    assert any("CYCLE TAKE PROFIT" in l["msg"] for l in tr.logs)


def test_t41u_profit_lock_closes_before_red(tmp_path):
    tr, br, ctl = _session(tmp_path, hedge_enabled=False, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    held = set(br.positions_)
    ctl["side"] = "NO TRADE"
    tr._micro_exit = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    tr._zone_target = lambda *a, **k: None
    tr.cfg.tp_target_money = 1e7                                      # target far away: test the lock alone
    tr._net = lambda legs, live: 1000.0                               # profit -> locked (peak 1000)
    run(tr, br, T0 + 3.3, T0 + 4)
    assert held <= set(br.positions_)
    tr._net = lambda legs, live: 500.0                                # gave back 50% (> 40%) -> close at once
    run(tr, br, T0 + 4.3, T0 + 5.2)
    assert not (held & set(br.positions_))
    assert any(("PROFIT LOCK" in l["msg"] or "TAKE PROFIT" in l["msg"]) for l in tr.logs if l["msg"].startswith("EXIT GROUP"))


def test_t42u_never_self_stops_and_guardian_watches(tmp_path):
    tr, br, ctl = _session(tmp_path, hedge_enabled=False, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    tr._net = lambda legs, live: -50.0
    tr.account["equity"] = 100.0
    from rapidburst.risk import AccountView
    assert tr.risk.update(T0 + 4, AccountView(equity=1.0, balance=1000.0, margin_free=1.0, margin_level=500.0)) is None
    assert not tr.risk.halted
    run(tr, br, T0 + 3.3, T0 + 6)
    assert tr.running and br.positions_                               # red, still running, nothing closed
    assert any(l["msg"].startswith("GUARD BTCUSDm") for l in tr.logs) and "BTCUSDm" in tr.guardian.view


def _green_session(tmp_path):
    tr, br, ctl = _session(tmp_path, hedge_enabled=False, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    ctl["side"] = "NO TRADE"
    tr._micro_exit = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    tr._zone_target = lambda *a, **k: None
    return tr, br, ctl, set(br.positions_)


def test_t43u_take_profit_at_target_grab(tmp_path):
    tr, br, ctl, held = _green_session(tmp_path)
    tr._net = lambda legs, live: 1e6                                   # far beyond tp_grab_x x any target -> grab now
    run(tr, br, T0 + 3.3, T0 + 4.2)
    assert not (held & set(br.positions_))
    assert any("TAKE PROFIT" in l["msg"] for l in tr.logs if l["msg"].startswith("EXIT GROUP"))


def test_t43u_early_close_on_opposite_signal_while_green(tmp_path):
    tr, br, ctl, held = _green_session(tmp_path)
    g = tr.bursts[next(iter(tr.pos_state.values())).batch_id]
    g["lock_peak"] = 0.0
    tr._net = lambda legs, live: 0.02                                  # thin green, below every lock level
    ctl["side"] = "SELL"
    tr.cfg.hedge_enabled = False
    run(tr, br, T0 + 3.3, T0 + 4.2)
    assert any("EARLY CLOSE" in l["msg"] for l in tr.logs if l["msg"].startswith("EXIT GROUP"))


def test_t43u_no_break_even_close_and_red_never_closed(tmp_path):
    tr, br, ctl, held = _green_session(tmp_path)
    for ps in tr.pos_state.values():                                   # an engine BE level that price is below
        ps.vsl, ps.sl_state = 1e9, "BE"
    tr._net = lambda legs, live: -0.5
    run(tr, br, T0 + 3.3, T0 + 6)
    assert held <= set(br.positions_)                                  # red: never closed (no BE close, no SL)
