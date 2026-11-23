"""Rapid Micro-Burst Scalping Engine: analysis, strength, decision, the single tick exit, hard-risk precedence,
group/batch rules and the autonomous loop. SimBroker + SYNTHETIC ticks only (test double, not market data)."""
import json

from rapidburst.config import EngineConfig
from rapidburst.engine import MomentumEngine
from rapidburst.microburst import Micro, decide, exit_reason
from tests.harness import Invariants, instrument, make, run
from tests.paths import T0, _force_signal, _small_account, _start, series_up
from tests.synth import burst_series, gen_ticks


def _analyze_stream(ticks, cfg=None, atr=1.0, after=None):
    """Feed ticks one by one and return [(t, Micro, Strength)] like the live loop (analysis at every tick)."""
    cfg = cfg or EngineConfig()
    eng = MomentumEngine("XAUUSD", 0.01, cfg)
    out = []
    for t, b, a in ticks:
        eng.on_tick(t, b, a)
        if after is not None and t < after:
            continue
        f = eng.features(t)
        m = eng.scanner.snapshot(t, atr, f.vol_unit, [], cfg, [])
        out.append((t, m, decide(m, cfg)))
    return out


def _live(**kw):
    base = dict(ready=True, t=100.0, mid=100.0, bid=99.95, ask=100.05, spread=0.1, atr=1.0, vu=0.15,
                tick_rate=3.0, rate_ratio=2.0, seq_run=5, seq_consist=0.8, vel=0.5, vel_prev=0.2, accel=0.3,
                eff=0.8, burst_z=3.0, burst_dir=1, act_ratio=2.0, body=0.3, body_range=0.8, close_loc=0.9,
                range_exp=1.5, breakout=1, compressed=True, imbalance=0.7, bidask=0.7, chase_up=0.3,
                chase_dn=0.0, origin_up=90.0, spread_ratio=1.0, target=0.3, stable=True, regime="fast")
    base.update(kw)
    return Micro(**base)


# --------------------------------------------------------------- analysis + decision
def test_up_burst_gives_buy_early_and_down_burst_gives_sell():
    up = _analyze_stream(burst_series(T0, +1), after=T0 + 140)
    first = next(((t, st) for t, m, st in up if st.side != "NO TRADE"), None)
    assert first and first[1].side == "BUY", first
    assert first[0] - (T0 + 150) < 2.0, f"entry signal {first[0] - (T0 + 150):.2f}s after the burst start"
    dn = _analyze_stream(burst_series(T0, -1, seed=7), after=T0 + 140)
    first = next(((t, st) for t, m, st in dn if st.side != "NO TRADE"), None)
    assert first and first[1].side == "SELL" and first[0] - (T0 + 150) < 2.0


def test_random_market_trades_are_the_exception():
    rows = _analyze_stream(gen_ticks(11, T0, 900), after=T0 + 60)
    fired = sum(st.side != "NO TRADE" for _, _, st in rows)
    assert rows and fired / len(rows) < 0.12, (fired, len(rows))


def test_strength_is_two_sided_and_explains_itself():
    st = decide(_live(), EngineConfig())
    assert st.side == "BUY" and st.buy > st.threshold and st.sell < st.buy
    lvl = EngineConfig().mb_scan_support
    assert st.scan["BUY"]["A"] >= lvl and max(st.scan["BUY"]["B"], st.scan["BUY"]["C"]) >= lvl
    assert st.reason.startswith("BUY") and "A" in st.reason
    assert "pre-burst" in decide(_live(early=1), EngineConfig()).reason
    st2 = decide(_live(vel=-0.5, accel=-0.3, eff=-0.8, seq_run=-5, seq_consist=-0.8, burst_dir=-1, body=-0.3,
                       close_loc=0.1, breakout=-1, imbalance=-0.7, bidask=-0.7, chase_up=0.0, chase_dn=0.3,
                       origin_dn=90.0), EngineConfig())
    assert st2.side == "SELL"


def test_anti_chase_and_big_bar_veto():
    cfg = EngineConfig()
    st = decide(_live(chase_up=5.0), cfg)
    assert st.side == "NO TRADE" and "anti-chase" in st.reason
    st = decide(_live(body=3.0), cfg)
    assert st.side == "NO TRADE" and "candle already" in st.reason


def test_no_live_force_and_unstable_feed_and_cost_block():
    cfg = EngineConfig()
    assert decide(_live(vel=0.0, accel=0.0), cfg).side == "NO TRADE"            # no candle/volume-only entries
    st = decide(_live(stable=False, gap_max=9.0), cfg)
    assert st.side == "NO TRADE" and "feed unstable" in st.reason
    st = decide(_live(spread_ratio=4.0, slip_risk=1.0), cfg)
    assert st.threshold > decide(_live(), cfg).threshold                        # costlier => higher bar


def test_tick_volume_and_depth_are_labelled_honestly():
    m = _live()
    assert m.depth.startswith("n/a")
    import rapidburst.microburst as mb
    assert "NOT exchange volume" in mb.__doc__ and "PROXY" in mb.__doc__


# --------------------------------------------------------------------- single exit
def _g(**kw):
    g = {"target": 0.3, "peak_pl": 0.0, "peak_vel": 0.5, "last_ext_t": 100.0}
    g.update(kw)
    return g


def test_momentum_hold_no_fixed_tp_and_small_retracement_is_held():
    cfg = EngineConfig()
    m = _live()                                                   # momentum running
    # far beyond the old "2x target" fixed capture: still held while momentum continues (no fixed TP)
    assert exit_reason(_g(peak_pl=0.7), m, 1, 0.7, 2, 0.0, 0.5, 0.8, 0.0, cfg) is None
    # small retracement (< ATR/spread noise floor) while running: held
    assert exit_reason(_g(peak_pl=0.7), m, 1, 0.62, 2, 0.0, 0.5, 0.8, 0.0, cfg) is None
    # a meaningful retracement alone (no exhaustion evidence) is still held: the BE/trailing stop protects it
    assert exit_reason(_g(peak_pl=0.7), m, 1, 0.35, 2, 0.0, 0.5, 0.8, 0.0, cfg) is None


def test_meaningful_retracement_with_exhaustion_protects_profit():
    cfg = EngineConfig()
    fading = _live(vel=0.05, vel_prev=0.3, accel=-0.25, t=110.0)            # decayed + stalled (no new high 10 s)
    r = exit_reason(_g(peak_pl=0.7), fading, 1, 0.35, 12, 0.0, 0.5, 0.8, 0.0, cfg)
    assert r and ("profit protection: retraced" in r or "momentum exhausted" in r), r


def test_exit_costs_are_netted():
    cfg = EngineConfig()
    fading = _live(vel=0.05, vel_prev=0.3, accel=-0.25, t=110.0, failed=-1, imbalance=-0.7)
    assert "in profit" in exit_reason(_g(peak_pl=0.6), fading, 1, 0.2, 12, 0.0, 0.5, 0.8, 0.0, cfg) or \
        "profit protection" in exit_reason(_g(peak_pl=0.6), fading, 1, 0.2, 12, 0.0, 0.5, 0.8, 0.0, cfg)
    r = exit_reason(_g(peak_pl=0.6), fading, 1, 0.2, 12, 0.0, 0.5, 0.8, 0.19, cfg)   # cost eats the profit
    assert r is None or "in profit" not in r


def test_losing_basket_is_held_inside_the_hard_stop_until_the_move_clearly_turns():
    cfg = EngineConfig()
    # a small loss on noise is NOT cut (the old 4 s "no follow-through" cut is gone)
    assert exit_reason(_g(), _live(vel=-0.01, regime="normal"), 1, -0.05, 5, 0.0, 0.5, 0.8, 0.0, cfg) is None
    # one structure failure alone: held
    assert exit_reason(_g(), _live(failed=-1), 1, -0.05, 2, 0.0, 0.5, 0.8, 0.0, cfg) is None
    # reversal + structure failure + more evidence: closed
    r = exit_reason(_g(), _live(failed=-1, vel=-0.2, imbalance=-0.7, t=110.0), 1, -0.05, 2, 0.6, 0.5, 0.8, 0.0, cfg)
    assert r and "directional reversal" in r, r
    # deep against with momentum against: cut before the hard stop
    r = exit_reason(_g(), _live(vel=-0.2, imbalance=-0.7, t=110.0, regime="normal"), 1, -0.45, 1, 0.0, 0.5, 0.8, 0.0, cfg)
    assert r and "momentum against" in r, r


# ------------------------------------------------------------------ autonomous loop
def _session(tmp_path, ticks, cfg=None, balance=1000.0, mpl=100.0, t_end=None):
    cfg = cfg or EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": ticks}, cfg)
    inv = Invariants()
    instrument(br, cfg, inv)
    _small_account(br, balance=balance, margin_per_lot=mpl)
    _start(tr, br, T0 + 1)
    orders = []
    orig = br.market_order

    def mo(s, side, v, sl, tp, dev, magic, c):
        r = orig(s, side, v, sl, tp, dev, magic, c)
        orders.append({"t": br.now, "side": side, "vol": v, "sl": sl, "tp": tp, "comment": c, "ok": r.get("ok")})
        return r
    br.market_order = mo
    run(tr, br, T0 + 1, t_end or ticks[-1][0])
    return tr, br, inv, orders


def test_loop_enters_the_burst_with_sl_no_tp_and_exits_the_group(tmp_path):
    tr, br, inv, orders = _session(tmp_path, series_up(), t_end=T0 + 200)
    burst = [o for o in orders if T0 + 150 <= o["t"] <= T0 + 158]
    assert burst and burst[0]["side"] == "BUY" and burst[0]["t"] - (T0 + 150) < 2.0, orders
    assert all(o["sl"] and o["tp"] == 0.0 for o in orders)                # broker SL, engine-driven exits
    assert inv.adds_into_loser == 0 and inv.opposite_while_open == 0
    exits = [l["msg"] for l in tr.logs if l["msg"].startswith("EXIT GROUP")]
    assert exits and all(p["sl"] for p in br.positions_.values())     # anything still held has its hard stop


def test_one_batch_per_signal_capped_same_lot_no_adds_later(tmp_path):
    tr, br, inv, orders = _session(tmp_path, series_up(), t_end=T0 + 260)
    groups = {}
    for o in orders:
        groups.setdefault(o["comment"].rsplit("-", 1)[0], []).append(o)
    cfg = EngineConfig()
    assert groups
    for legs in groups.values():
        assert len(legs) <= cfg.max_orders_per_batch
        assert len({o["side"] for o in legs}) == 1
        assert len({o["vol"] for o in legs}) == 1                              # same lot: no martingale
        assert sum(o["vol"] for o in legs) <= cfg.max_batch_lots + 1e-9
        assert max(o["t"] for o in legs) - min(o["t"] for o in legs) < 1e-6    # one loop: never added later
    assert inv.adds_into_loser == 0 and inv.max_pos <= cfg.max_positions_total
    assert any(l["msg"].startswith("BATCH ") for l in tr.logs)


def test_duplicate_burst_is_not_traded_twice(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    for t in list(br.positions_):
        br.positions_.pop(t)
    tr.pos_state.clear()
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    tr.pacer = type(tr.pacer)(tr.cfg)
    n0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 100.3, T0 + 101)
    assert len([l for l in tr.logs if l["msg"].startswith("ENTRY")]) >= n0 + 1
    for t in list(br.positions_):                    # the group ends (e.g. broker SL) ...
        br.close_position(t, 50, tr.cfg.magic)
    n1 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 101.3, T0 + 110)                 # ... the same burst must not be re-entered
    assert not [l for l in tr.logs if l["msg"].startswith("ENTRY")][n1:]
    assert tr.skip_counts["BTCUSDm"]["duplicate burst"] >= 1


def test_hard_risk_exit_overrides_the_micro_exit(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 100.3, T0 + 101)
    assert br.positions_
    calls = []
    tr._hard_risk_exit = lambda *a, **k: "hard group loss (test)"
    tr._micro_exit = lambda *a, **k: calls.append(1) or "micro exit"
    run(tr, br, T0 + 101.3, T0 + 102)
    assert not br.positions_ and not calls
    assert any(l["msg"].startswith("EXIT GROUP") and "hard group loss" in l["msg"] for l in tr.logs)


def test_no_daily_loss_halt_and_safe_mode_defaults(monkeypatch):
    for k in ("RB_DAILY_LOSS_PCT", "RB_MAX_DD_PCT", "RB_CONFIG_JSON"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("RB_DAILY_LOSS_PCT", "3")                    # stale env var: ignored
    c = EngineConfig.load()
    assert not hasattr(c, "daily_loss_limit_pct") and c.equity_drawdown_pct == 8.0
    assert c.max_margin_use_pct == 50.0 and c.risk_per_trade_pct == 0.5 and c.max_total_risk_pct == 3.0
    assert c.max_orders_per_batch == 8 and c.risk_per_batch_pct == 1.0


def test_safe_mode_flattens_pauses_entries_and_keeps_running(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 151)
    tr.risk.halted, tr.risk.halt_reason, tr.risk.safe_until = True, "SAFE MODE: (test)", T0 + 10000
    n0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 151.3, T0 + 200)
    assert not br.positions_ and tr.running
    assert len([l for l in tr.logs if l["msg"].startswith("ENTRY")]) == n0
    assert "SAFE MODE" in tr.sym_status["BTCUSDm"]["reason"]


def test_stop_flattens_bot_positions(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    opened = []

    def on(t):
        if br.positions_ and not opened:
            opened.append(t)
            tr.cmd_stop()
    run(tr, br, T0 + 1, T0 + 200, on_step=on)
    assert opened and not br.positions_ and not tr.running


def test_margin_cap_and_risk_based_lot(tmp_path):
    cfg = EngineConfig(max_margin_use_pct=0.5)       # 0.5% of $1000 = $5 < $10 margin per 0.01 lot at 1000/lot
    tr, br, inv, orders = _session(tmp_path, series_up(), cfg=cfg, mpl=1000.0, t_end=T0 + 200)
    assert not orders
    assert any("margin" in k for k in tr.skip_counts.get("BTCUSDm", {})), dict(tr.skip_counts.get("BTCUSDm", {}))


def test_unprotected_positions_flattened_when_engine_not_running(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    br.advance(T0 + 200)
    br.positions_[7] = {"ticket": 7, "symbol": "BTCUSDm", "type": 0, "volume": 0.01, "price_open": 2650.0,
                        "price_current": 2650.0, "sl": 0.0, "tp": 0.0, "profit": 0.0, "swap": 0.0,
                        "magic": cfg.magic, "comment": "RBurst-old", "time": T0}
    for i in range(4):
        tr.step(T0 + 200 + i)
    assert not br.positions_
    assert any("flattening immediately" in l["msg"] for l in tr.logs)


def test_snapshot_has_one_engine_view(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 152)
    tr._build_snapshot(T0 + 152)
    e = tr.get_snapshot()["engine"]
    assert "groups" in e and "baskets" not in e and "scalp" not in e
    rc = e["risk_config"]
    assert rc["engine"].startswith("Immediate Continuous Score Momentum Scalper") and rc["broker_sl"] == "ON"
    assert "daily_loss_limit_pct" not in rc and "latency" in e and "performance" in e
    assert e["performance"]["source"] == "real broker deals"
    st = e["symbols"]["BTCUSDm"]
    assert "strength" in st and {"buy", "sell", "threshold"} <= set(st["strength"])


def test_config_json_env_override_ignores_unknown_and_old_keys(monkeypatch):
    monkeypatch.setenv("RB_CONFIG_JSON", json.dumps({"mb_threshold": 0.6, "basket_enabled": True, "batch_max": 9}))
    c = EngineConfig.load()
    assert c.mb_threshold == 0.6 and not hasattr(c, "basket_enabled")


def test_rsi_is_a_soft_feature_never_a_gate_and_mtf_is_part_of_the_single_decision():
    import inspect
    import rapidburst.microburst as mb
    import rapidburst.mtf as mtf
    from rapidburst.mtf import TFState, rsi_score
    src = inspect.getsource(mb.decide)
    assert "rsi" not in src.lower()                           # no RSI condition / cross wait in the decision
    assert "rsi_score" in inspect.getsource(mtf.m1_timing)     # only a weighted part of M1 timing
    s = TFState(ready=True, rsi_ready=True, rsi=45.0, rsi_slope=-1.0)
    assert 0.0 <= rsi_score(s, 1) <= 1.0
    assert "_mtf_scores" in inspect.getsource(mb.side_strength)
