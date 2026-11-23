"""M5 bias / M1 timing: incremental MACD vs brute force, broker-bar merge, bias/timing behaviour, decision and
exit integration, candle polling rate, continuous re-entry, real-only stats. SYNTHETIC bars/ticks (test double)."""
import math

from rapidburst.config import EngineConfig
from rapidburst.microburst import Micro, decide, exit_reason
from rapidburst.mtf import MTFView, TF, TFState, m1_timing, m5_bias
from tests.harness import make, run
from tests.paths import T0, _force_signal, _small_account, _start, series_up


def _bars(closes, t0=1_800_000_000, sec=300, rng=1.0, vol=100):
    out, prev = [], closes[0]
    for i, c in enumerate(closes):
        o = prev
        out.append({"time": t0 + i * sec, "open": o, "high": max(o, c) + rng / 4, "low": min(o, c) - rng / 4,
                    "close": c, "tick_volume": vol})
        prev = c
    return out


def _ema(xs, n):
    a, e, out = 2 / (n + 1), xs[0], []
    for x in xs:
        e = e + a * (x - e)
        out.append(e)
    return out


def test_incremental_macd_matches_brute_force():
    closes = [100 + math.sin(i / 7) * 3 + i * 0.05 for i in range(120)]
    tf = TF(300)
    bars = _bars(closes)
    tf.load(bars + [dict(bars[-1], time=bars[-1]["time"] + 300)], bars[-1]["time"] + 300 + 10)
    e12, e26 = _ema(closes, 12), _ema(closes, 26)
    macd = [a - b for a, b in zip(e12, e26)]
    sig = [macd[0]]
    for m in macd[1:]:
        sig.append(sig[-1] + 2 / 10 * (m - sig[-1]))
    assert abs(tf.st.macd - macd[-1]) < 1e-9 and abs(tf.st.hist - (macd[-1] - sig[-1])) < 1e-9


def test_broker_bars_merge_with_live_ticks_and_corrections_rebuild():
    t0 = 1_800_000_000
    tf = TF(60)
    for i in range(0, 600, 2):                      # 10 minutes of ticks -> bars built from ticks
        tf.on_tick(t0 + i, 100 + i * 0.001)
    assert len(tf.bars) == 9 and tf.live["time"] == t0 + 540
    broker = _bars([100 + k * 0.06 for k in range(10)], t0=t0, sec=60)
    broker[3]["close"] += 0.5                       # broker's real bar differs from the tick-built one
    tf.load(broker, t0 + 545)
    assert len(tf.bars) == 9 and abs(tf.bars[3]["close"] - broker[3]["close"]) < 1e-12
    assert tf.bars[3]["own"] is not None            # own live tick count kept for like-for-like volume
    st = tf.state(t0 + 545)
    assert st.ready and st.src == "broker+ticks"


def test_m5_bias_up_down_ambiguous_and_m1_timing():
    up = TF(300)
    b = _bars([100 + i * 0.4 for i in range(60)])
    up.load(b, b[-1]["time"] + 300)
    for k in range(30):
        up.on_tick(b[-1]["time"] + 300 + k * 5, b[-1]["close"] + k * 0.03)
    s = up.state(b[-1]["time"] + 450)
    bias, lab, sup = m5_bias(s)
    assert bias > 0.3 and lab.startswith("up")
    dn = TF(300)
    b2 = _bars([100 - 0.4 * i - 0.01 * i * i for i in range(60)])   # accelerating sell-off
    dn.load(b2, b2[-1]["time"] + 300)
    dn.on_tick(b2[-1]["time"] + 310, b2[-1]["close"] - 0.2)
    assert m5_bias(dn.state(b2[-1]["time"] + 320))[0] < -0.3
    flat = TF(300)
    b3 = _bars([100 + (0.2 if i % 2 else -0.2) for i in range(60)])
    flat.load(b3, b3[-1]["time"] + 300)
    flat.on_tick(b3[-1]["time"] + 310, 100.0)
    assert "ambiguous" in m5_bias(flat.state(b3[-1]["time"] + 320))[1]
    assert m1_timing(s, 1, bias) > m1_timing(s, -1, bias)


def _live(**kw):
    base = dict(ready=True, t=100.0, mid=100.0, bid=99.95, ask=100.05, spread=0.1, atr=1.0, vu=0.15,
                tick_rate=3.0, rate_ratio=2.0, seq_run=5, seq_consist=0.8, vel=0.5, vel_prev=0.2, accel=0.3,
                eff=0.8, burst_z=3.0, burst_dir=1, act_ratio=2.0, body=0.3, body_range=0.8, close_loc=0.9,
                range_exp=1.5, breakout=1, compressed=True, imbalance=0.7, bidask=0.7, chase_up=0.3,
                chase_dn=0.0, origin_up=90.0, spread_ratio=1.0, target=0.3, stable=True, regime="fast")
    base.update(kw)
    return Micro(**base)


def _view(bias, supported=True, slope=0.1, accel=0.05, vol=1.5, exhaust=0):
    v = MTFView(m5=TFState(ready=True, exhaust=exhaust), m1=TFState(ready=True, hist_slope=slope, hist_accel=accel,
                                                                    vol_x=vol, body=0.4, range_x=1.0))
    v.bias, v.supported = bias, supported
    v.bias_label = "test"
    v.m1_up, v.m1_dn = m1_timing(v.m1, 1, bias), m1_timing(v.m1, -1, bias)
    return v


def test_m5_is_a_filter_not_a_gate():
    cfg = EngineConfig()
    with_m5 = decide(_live(mtf=_view(0.7)), cfg)
    amb = decide(_live(mtf=_view(-0.3, supported=False)), cfg)
    against = decide(_live(mtf=_view(-0.8, supported=True)), cfg)
    assert with_m5.side == "BUY" and "D" in with_m5.reason and "E" in with_m5.reason
    assert amb.buy < with_m5.buy                       # ambiguous / weak opposite M5 only costs strength
    assert against.side == "NO TRADE" and "M5 clearly against" in against.reason


def test_exit_needs_combined_exhaustion_in_profit_not_one_signal():
    cfg = EngineConfig()
    g = {"target": 0.3, "peak_pl": 0.12, "peak_vel": 0.5, "last_ext_t": 100.0}
    # MACD histogram turn + volume weakening only (2 signals): momentum hold, no close on that alone
    two = _live(vel=0.1, vel_prev=0.4, accel=-0.3, mtf=_view(0.5, slope=-0.1, accel=-0.05, vol=0.6))
    assert exit_reason(g, two, 1, 0.1, 3, 0.0, 0.5, 0.8, 0.0, cfg) is None
    # + the M1 candle turning against (3 signals agree) -> close in profit
    v = _view(0.5, slope=-0.1, accel=-0.05, vol=0.6)
    v.m1.body = -0.3
    three = _live(vel=0.1, vel_prev=0.4, accel=-0.3, mtf=v)
    r = exit_reason(g, three, 1, 0.1, 3, 0.0, 0.5, 0.8, 0.0, cfg)
    assert r and "momentum exhausted in profit" in r and "MACD-hist-turn" in r, r
    running = _live(mtf=_view(0.5, slope=0.1, accel=0.05))
    assert exit_reason(g, running, 1, 0.1, 3, 0.0, 0.5, 0.8, 0.0, cfg) is None


def test_candles_polled_at_a_sensible_rate_not_every_loop(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    calls = []

    def candles(s, tf, hours):
        calls.append((br.now, tf, hours))
        return []
    br.candles = candles
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 121)                    # 400 loops
    m1 = [c for c in calls if c[1] == "M1"]
    m5 = [c for c in calls if c[1] == "M5"]
    assert 5 <= len(m1) <= 8 and 2 <= len(m5) <= 3, (len(m1), len(m5))
    assert all(isinstance(h, int) for _, _, h in calls)   # integer look-back hours


def test_continuous_reentry_right_after_a_basket_closes_on_a_fresh_signal(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    for t in list(br.positions_):
        br.close_position(t, 50, tr.cfg.magic)
    run(tr, br, T0 + 100.0, T0 + 100.1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    real = tr._micro
    k = [0]

    def fresh(s, now_wall):                         # a new setup every loop (new burst origin)
        m, st = real(s, now_wall)
        k[0] += 1
        st.origin = float(k[0])
        return m, st
    tr._micro = fresh
    run(tr, br, T0 + 100.3, T0 + 100.6)
    assert br.positions_
    tr._hard_risk_exit = lambda *a, **kw: "hard group loss (test)"
    run(tr, br, T0 + 100.9, T0 + 100.9)
    tr._hard_risk_exit = lambda *a, **kw: None
    tr._micro_exit = lambda *a, **kw: None
    run(tr, br, T0 + 101.2, T0 + 101.5)
    assert tr.reentry and min(tr.reentry) <= 0.31, list(tr.reentry)   # next basket within one loop


def test_stats_come_only_from_closed_baskets_with_attribution(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    tr._build_snapshot(T0 + 1)
    assert tr.get_snapshot()["engine"]["performance"] == {"batches": 0, "source": "real broker deals"}
    run(tr, br, T0 + 1, T0 + 260)
    pf = tr._perf_view()
    if pf["batches"]:
        assert {"win_rate", "profit_factor", "expectancy", "avg_win", "avg_loss", "orders", "batches_per_hour",
                "reentry_s", "avg_spread_pts", "avg_slip_pts", "attribution", "max_drawdown"} <= set(pf)
        assert set(pf["attribution"]) == {"m5", "macd", "volume", "tick"}
