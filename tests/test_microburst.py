"""Rapid Micro-Burst Scalping Engine: analysis, strength, decision, the single tick exit, hard-risk precedence,
group/batch rules and the autonomous loop. SimBroker + SYNTHETIC ticks only (test double, not market data)."""
import json

from rapidburst.config import EngineConfig
from rapidburst.engine import MomentumEngine
from rapidburst.microburst import Micro, analyze, decide, exit_reason
from rapidburst.trend import compute_trend
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
        m = analyze(list(eng.buf.ticks), t, atr, f.vol_unit, eng.spreads, [], cfg, [])
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
    assert st.support["BUY"] >= EngineConfig().mb_min_support and "micro-burst BUY" in st.reason
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


def test_exit_profit_capture_and_giveback():
    cfg = EngineConfig()
    m = _live()
    assert "2x target" in exit_reason(_g(), m, 1, 0.7, 2, 0.0, 0.5, 0.8, 0.0, cfg)
    r = exit_reason(_g(), _live(vel=0.05), 1, 0.35, 2, 0.0, 0.5, 0.8, 0.0, cfg)          # target + velocity decay
    assert r and r.startswith("profit capture"), r
    r = exit_reason(_g(peak_pl=0.5), m, 1, 0.25, 2, 0.0, 0.5, 0.8, 0.0, cfg)             # gave back 50%
    assert r and "give-back" in r, r
    assert exit_reason(_g(), m, 1, 0.2, 2, 0.0, 0.5, 0.8, 0.0, cfg) is None              # running, below target


def test_exit_costs_are_netted():
    cfg = EngineConfig()
    assert exit_reason(_g(), _live(), 1, 0.7, 2, 0.0, 0.5, 0.8, 0.0, cfg)
    assert exit_reason(_g(), _live(), 1, 0.7, 2, 0.0, 0.5, 0.8, 0.45, cfg) is None or \
        "2x target" not in exit_reason(_g(), _live(), 1, 0.7, 2, 0.0, 0.5, 0.8, 0.45, cfg)


def test_exit_micro_reversal_failed_breakout_and_no_follow_through():
    cfg = EngineConfig()
    assert "failed breakout" in exit_reason(_g(), _live(failed=-1), 1, -0.05, 2, 0.0, 0.5, 0.8, 0.0, cfg)
    assert "structure break" in exit_reason(_g(), _live(struct_break=-1), 1, -0.05, 2, 0.0, 0.5, 0.8, 0.0, cfg)
    assert "opposite strength" in exit_reason(_g(), _live(), 1, -0.05, 2, 0.6, 0.5, 0.8, 0.0, cfg)
    r = exit_reason(_g(), _live(vel=-0.01, regime="normal"), 1, -0.05, 5, 0.0, 0.5, 0.8, 0.0, cfg)
    assert r and "no follow-through" in r, r
    r = exit_reason(_g(), _live(vel=-0.2, imbalance=-0.7, regime="normal"), 1, -0.45, 1, 0.0, 0.5, 0.8, 0.0, cfg)
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
    assert exits and not br.positions_


def test_adds_only_same_side_same_lot_capped_and_never_into_a_loser(tmp_path):
    tr, br, inv, orders = _session(tmp_path, series_up(), t_end=T0 + 260)
    groups = {}
    for o in orders:
        groups.setdefault(o["comment"].rsplit("-", 1)[0], []).append(o)
    cfg = EngineConfig()
    for legs in groups.values():
        assert len(legs) <= cfg.max_orders_per_group
        assert len({o["side"] for o in legs}) == 1
        assert all(o["vol"] <= legs[0]["vol"] + 1e-12 for o in legs)          # no martingale / no size-up
    assert inv.adds_into_loser == 0 and inv.max_pos <= cfg.max_positions_total


def test_duplicate_burst_is_not_traded_twice(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    for t in list(br.positions_):
        br.positions_.pop(t)
    tr.pos_state.clear()
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    tr.group_cooldown.clear()
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


def test_daily_loss_and_drawdown_halts_are_on_by_default(monkeypatch):
    for k in ("RB_DAILY_LOSS_PCT", "RB_MAX_DD_PCT", "RB_CONFIG_JSON"):
        monkeypatch.delenv(k, raising=False)
    c = EngineConfig.load()
    assert c.daily_loss_limit_pct == 5.0 and c.equity_drawdown_pct == 8.0
    assert c.max_margin_use_pct == 50.0 and c.risk_per_trade_pct == 0.5 and c.max_total_risk_pct == 3.0
    monkeypatch.setenv("RB_DAILY_LOSS_PCT", "3")
    assert EngineConfig.load().daily_loss_limit_pct == 3.0


def test_daily_loss_halt_flattens_and_blocks_entries(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 151)
    tr.risk.halted, tr.risk.halt_reason = True, "daily loss limit (test)"
    n0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 151.3, T0 + 200)
    assert not br.positions_
    assert len([l for l in tr.logs if l["msg"].startswith("ENTRY")]) == n0


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
    assert rc["engine"].startswith("Rapid Micro-Burst") and rc["broker_sl"] == "ON"
    st = e["symbols"]["BTCUSDm"]
    assert "strength" in st and {"buy", "sell", "threshold"} <= set(st["strength"])


def test_config_json_env_override_ignores_unknown_and_old_keys(monkeypatch):
    monkeypatch.setenv("RB_CONFIG_JSON", json.dumps({"mb_threshold": 0.6, "basket_enabled": True, "batch_max": 9}))
    c = EngineConfig.load()
    assert c.mb_threshold == 0.6 and not hasattr(c, "basket_enabled")


def test_trend_is_context_only():
    def bars(closes, rng=1.0):
        return [{"time": i * 60.0, "open": c, "high": c + rng / 2, "low": c - rng / 2, "close": c}
                for i, c in enumerate(closes)]
    up = compute_trend(bars([100 + i * 0.6 for i in range(30)]), bars([100 + i * 2 for i in range(12)], 3))
    assert up.known and up.dir == 1
    import inspect
    import rapidburst.microburst as mb
    assert "trend" not in inspect.getsource(mb.decide)
