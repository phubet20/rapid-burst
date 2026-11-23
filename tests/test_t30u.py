"""M1 Rapid Basket Scalper (t30u): M1-only trend with HH/HL swings, >= 2-order baskets, pyramid adds only after
break-even, spike exit / pullback hold, re-entry after a break-even stop, aggressive (owner-requested) sizing.
SimBroker + SYNTHETIC ticks/bars only (test double, not market results)."""
from types import SimpleNamespace

from rapidburst.config import EngineConfig
from rapidburst.microburst import exit_reason
from rapidburst.mtf import TF, m1_trend
from tests.harness import make, run
from tests.paths import T0, _force_signal, _small_account, _start, series_up
from tests.test_mtf import _live, _view


def _bars(highs_lows, t0=1_800_000_000):
    out = []
    for i, (h, lo) in enumerate(highs_lows):
        out.append({"time": t0 + 60 * i, "open": (h + lo) / 2, "high": h, "low": lo, "close": (h + lo) / 2,
                    "tick_volume": 50})
    return out


def test_m1_swing_structure_hh_hl_and_break():
    zig = []
    base = 100.0
    for k in range(10):                                   # rising zig-zag: higher highs and higher lows
        zig += [(base + 1.0, base), (base + 2.0, base + 0.8), (base + 1.4, base + 0.6)]
        base += 0.7
    tf = TF(60)
    bars = _bars(zig)
    tf.load(bars + [dict(bars[-1], time=bars[-1]["time"] + 60, close=bars[-1]["high"] + 5)], bars[-1]["time"] + 70)
    st = tf.state(bars[-1]["time"] + 70)
    assert st.swing == 1 and st.sbreak == 1
    b, label, _ = m1_trend(st)
    assert b > 0 and "HH/HL" in label


def test_spike_turning_back_closes_and_a_pullback_in_the_trend_is_held():
    cfg = EngineConfig()
    g = {"target": 0.3, "peak_pl": 0.9, "peak_vel": 0.5, "last_ext_t": 100.0}
    turn = _live(vel=-0.3, accel=-0.3, imbalance=-0.7, bidask=-0.7, mtf=_view(0.5))
    r = exit_reason(g, turn, 1, 0.5, 3, 0.0, 0.5, 0.8, 0.0, cfg)
    assert r and r.startswith("spike turning back"), r
    calm = _live(vel=0.05, accel=0.0, imbalance=0.1, bidask=0.1, mtf=_view(0.6))
    g2 = {"target": 0.3, "peak_pl": 0.5, "peak_vel": 0.5, "last_ext_t": 100.0}
    assert exit_reason(g2, calm, 1, 0.35, 3, 0.0, 0.5, 0.8, 0.0, cfg) is None       # pullback held


def test_default_basket_has_at_least_two_orders(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())          # t30u defaults
    _small_account(br, balance=10.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 1, T0 + 30)
    batches = [l["msg"] for l in tr.logs if l["msg"].startswith("BATCH")]
    assert batches and all(int(b.split(": ")[1].split("/")[0]) >= 2 for b in batches), batches[:3]


def test_margin_cap_shrinks_the_lot_not_the_safety(tmp_path):
    cfg = EngineConfig(max_margin_use_pct=20.0)
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, cfg)
    _small_account(br, balance=100_000.0, margin_per_lot=5000.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 1, T0 + 5)
    used = sum(p["volume"] * 5000.0 for p in br.positions_.values())
    assert br.positions_ and used <= 0.2 * 100_000.0 + 1e-6
    assert all(p["sl"] for p in br.positions_.values())                      # hard stop on every order


def _legs(side="BUY", n=2, price=100.0):
    return [SimpleNamespace(symbol="BTCUSDm", side=side, volume=0.01, batch_id="g1", open_price=price) for _ in range(n)]


def test_pyramid_add_only_after_break_even_on_a_resumed_pullback(tmp_path):
    tr, _ = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())
    st = SimpleNamespace(side="BUY")
    m = SimpleNamespace(ready=True, atr=1.0, mid=101.0, vel=0.2)
    g = {"ext_px": 101.0}
    assert "break-even" in tr._add_check("BTCUSDm", _legs(), g, m, st, 1000.0)       # no BE: never adds
    g["prot"] = "BE"
    assert tr._add_check("BTCUSDm", _legs(), g, m, st, 1000.0) == "no pullback yet"
    m.mid, m.vel = 100.8, -0.1                                                          # pullback 0.2 ATR
    assert "waiting for price to resume" in tr._add_check("BTCUSDm", _legs(), g, m, st, 1000.0)
    m.mid, m.vel = 100.95, 0.2                                                          # resumes
    assert tr._add_check("BTCUSDm", _legs(), g, m, st, 1000.0) is None
    assert "not valid" in tr._add_check("BTCUSDm", _legs(), g, m, SimpleNamespace(side="SELL"), 1000.0)
    assert "full" in tr._add_check("BTCUSDm", _legs(n=8), g, m, st, 1000.0)


def test_break_even_stop_exit_does_not_block_reentry(tmp_path):
    tr, _ = make(tmp_path, {"BTCUSDm": series_up()})
    tr.last_burst["BTCUSDm"] = {"gid": "g1", "side": "BUY"}
    ps = SimpleNamespace(symbol="BTCUSDm", batch_id="g1", volume=0.01, side="BUY")
    b = {"realized": -0.01, "prot": "BE", "lot": 0.01, "side": "BUY", "opened": 0.0, "legs": 2, "tickets": []}
    tr._finish_basket(ps, b, 10.0)
    assert tr.last_burst["BTCUSDm"]["lost"] is False
    tr.last_burst["BTCUSDm"] = {"gid": "g2", "side": "BUY"}
    ps.batch_id = "g2"
    tr._finish_basket(ps, dict(b, prot=None), 11.0)
    assert tr.last_burst["BTCUSDm"]["lost"] is True


def test_setup_key_uses_m1_trend_and_swing():
    m = SimpleNamespace(mtf=_view(0.6), regime="fast", spread=0.1, atr=1.0)
    m.mtf.m1.swing = 1
    from rapidburst.trader import Trader
    assert Trader._setup_key("XAUUSDm", "BUY", m) == "XAUUSDm|with+swing|fast|tight"
    assert Trader._setup_key("XAUUSDm", "SELL", m) == "XAUUSDm|counter-swing|fast|tight"


def test_add_to_basket_sends_same_lot_orders_into_the_open_basket(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    tr._micro_exit = lambda *a, **k: None
    tr._hard_risk_exit = lambda *a, **k: None
    run(tr, br, T0 + 1, T0 + 5)
    legs = [ps for ps in tr.pos_state.values() if ps.ticket in br.positions_]
    assert len(legs) >= 2
    gid = legs[0].batch_id
    g = tr.bursts[gid]
    for p in tr.own_positions:                          # every leg in profit (no averaging down)
        p["profit"] = 1.0
    m, st = tr._micro("BTCUSDm", T0 + 5)
    # t32u: an add only goes in when it can SHARE the basket's BE/trail stop
    g["prot"], g["prot_level"] = "BE", 0.0
    status = {}
    assert not tr._add_to_basket("BTCUSDm", legs, g, m, st, tr.sym_info["BTCUSDm"], status, T0 + 5, T0 + 5)
    assert "shared break-even/trail stop" in status["reason"]
    g["last_add"] = 0.0
    g["prot_level"] = round(m.bid - 2 * m.spread, 2)
    n0 = len(br.positions_)
    status = {}
    ok = tr._add_to_basket("BTCUSDm", legs, g, m, st, tr.sym_info["BTCUSDm"], status, T0 + 5, T0 + 5)
    assert ok, status
    assert all(p["sl"] == g["prot_level"] for p in br.positions_.values() if "-a" in p["comment"])
    assert ok and len(br.positions_) > n0
    lots = {p["volume"] for p in br.positions_.values()}
    assert len(lots) == 1 and all(p["sl"] for p in br.positions_.values())          # same lot, hard stop
    assert any(l["msg"].startswith("ADD basket") for l in tr.logs) and g["adds"] >= 1


def test_young_loser_inside_the_m1_trend_is_not_cut_on_micro_noise():
    cfg = EngineConfig()
    g = {"target": 0.3, "peak_pl": 0.0, "peak_vel": 0.5, "last_ext_t": 0.0}
    kw = dict(vel=-0.3, accel=-0.3, imbalance=-0.7, bidask=-0.7, failed=-1, rejection=-1)
    v = _view(0.6)                                       # M1 trend with the BUY basket, no swing break
    m = _live(mtf=v, **kw)
    assert exit_reason(g, m, 1, -0.1, 3, 0.9, 0.5, 0.8, 0.0, cfg) is None          # small loss: hold
    v2 = _view(0.6)
    v2.m1.sbreak = -1                                    # the M1 swing low broke: the trend case is over
    r = exit_reason(g, _live(mtf=v2, **kw), 1, -0.1, 3, 0.9, 0.5, 0.8, 0.0, cfg)
    assert r and "structure invalidated" in r


def test_t32u_trailing_basket_in_intact_trend_is_left_to_the_trail():
    cfg = EngineConfig()
    g = {"target": 0.3, "peak_pl": 0.9, "peak_vel": 0.5, "last_ext_t": 100.0, "prot": "trail"}
    turn = _live(vel=-0.3, accel=-0.3, imbalance=-0.7, bidask=-0.7, mtf=_view(0.5))
    assert exit_reason(g, turn, 1, 0.5, 3, 0.0, 0.5, 0.8, 0.0, cfg) is None             # the trail manages
    v = _view(0.5)
    v.m1.sbreak = -1                                                                   # structure broke
    r = exit_reason(dict(g), _live(vel=-0.3, accel=-0.3, imbalance=-0.7, bidask=-0.7, mtf=v), 1, 0.5, 3, 0.0,
                    0.5, 0.8, 0.0, cfg)
    assert r is not None
