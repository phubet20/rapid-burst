"""t32u: conflict fixes, indicator confluence layer, structure stop, balance-peak safe mode, learned weights."""
from types import SimpleNamespace

from rapidburst import indicators as ind
from rapidburst.config import EngineConfig, validate_tune
from rapidburst.learner import Learner
from rapidburst.risk import AccountView, RiskGuard
from rapidburst.trader import Trader
from tests.test_mtf import _live, _view
from rapidburst.microburst import decide


def _bars(closes, t0=1_700_000_000.0, wig=0.3):
    out, prev = [], closes[0]
    for i, c in enumerate(closes):
        out.append({"time": t0 + 60 * i, "open": prev, "high": max(prev, c) + wig, "low": min(prev, c) - wig,
                    "close": c, "vol": 100.0 + (i % 7)})
        prev = c
    return out


def test_indicators_trend_up_votes_up_and_down_votes_down():
    up = ind.compute(_bars([100 + 0.5 * i for i in range(90)]))
    v = ind.view(up, up.emas[8] + 0.5, None)
    assert v.ready and v.score(1) > 0.6 > 0.4 > v.score(-1)
    assert v.votes["ribbon"] > 0.9 and v.votes["supertrend"] > 0 and v.votes["adx"] > 0
    dn = ind.compute(_bars([200 - 0.5 * i for i in range(90)]))
    w = ind.view(dn, dn.emas[8] - 0.5, None)
    assert w.score(-1) > 0.6 and w.votes["ribbon"] < -0.9
    assert set(v.votes) == set(ind.NAMES)


def test_indicators_chop_and_sr_proximity():
    flat = ind.compute(_bars([100 + (0.2 if i % 2 else -0.2) for i in range(90)], wig=0.1))
    v = ind.view(flat, 100.0, None)
    assert v.chop
    iv = ind.IndView(ready=True, votes={"ribbon": 1.0}, res_above=101.0, sup_below=95.0)
    assert ind.sr_block(iv, 1, 100.9, 1.0, 0.2)                  # BUY right under resistance: blocked
    assert ind.sr_block(iv, -1, 100.9, 1.0, 0.2) is None          # SELL there is fine
    iv.breakout = 1
    assert ind.sr_block(iv, 1, 100.9, 1.0, 0.2) is None           # breaking out: allowed


def test_confluence_family_f_vetoes_against_and_requires_d():
    cfg = EngineConfig()
    good = ind.IndView(ready=True, votes={k: 0.8 for k in ind.NAMES}, adx=30.0)
    bad = ind.IndView(ready=True, votes={k: -0.8 for k in ind.NAMES}, adx=30.0)
    st = decide(_live(mtf=_view(0.7), ind=good), cfg)
    assert st.side == "BUY" and "F" in st.scan["BUY"] and st.scan["BUY"]["F"] > 0.8
    st2 = decide(_live(mtf=_view(0.7), ind=bad), cfg)
    assert st2.side == "NO TRADE" and "confluence" in st2.reason
    chop = ind.IndView(ready=True, votes={k: 0.5 for k in ind.NAMES}, chop=True, chop_why="ADX 12 < 18")
    plain = ind.IndView(ready=True, votes={k: 0.5 for k in ind.NAMES})
    # t34u: chop is a small penalty, not a veto (ind_chop_veto restores the veto)
    assert decide(_live(mtf=_view(0.7), ind=chop), cfg).buy < decide(_live(mtf=_view(0.7), ind=plain), cfg).buy
    cfg.ind_chop_veto = True
    assert "chop" in decide(_live(mtf=_view(0.7), ind=chop), cfg).reason


def test_safe_mode_peak_is_closed_balance_not_a_floating_spike():
    g = RiskGuard(EngineConfig(equity_drawdown_pct=30.0))
    g.reset_session(50.0)
    assert g.update(0, AccountView(50.0, 90.0, 80, 0)) is None     # +40 floating: NOT a new peak
    assert g.peak_equity == 50.0
    assert g.update(1, AccountView(50.0, 40.0, 30, 0)) is None     # basket gives it back: no safe mode
    ev = g.update(2, AccountView(50.0, 34.0, 30, 0))               # 32 % below the closed peak 50
    assert ev[0] == "safe" and "NEW entries paused" in ev[1] and not g.catastrophic
    ev = g.update(10_000, AccountView(50.0, 45.0, 40, 0))
    assert ev[0] == "resume" and g.need_confirm
    ev = g.update(10_001, AccountView(50.0, 19.0, 10, 0))          # > 60 % below the session peak 50
    assert ev[0] == "safe" and "CATASTROPHIC" in ev[1] and g.catastrophic and g.halted
    assert not g.check_entry("BTCUSDm", "BUY", [], AccountView(50.0, 19.0, 10, 0), 0.01, 1, 1, True)[0]
    assert g.update(10_001 + 1801, AccountView(19.0, 19.0, 10, 0))[0] == "resume"
    assert not g.catastrophic and g.session_peak == 19.0             # restarts from the new balance


def test_structure_stop_beyond_swing_clamped_to_atr():
    tr = Trader.__new__(Trader)
    tr.cfg = EngineConfig()
    v = _view(0.6)
    v.m1.swing_lo, v.m1.swing_hi = 99.2, 101.5
    m = SimpleNamespace(atr=1.0, spread=0.1, ask=100.05, bid=99.95, mtf=v)
    d, src = tr._sl_distance("BUY", m, 0.01)
    assert src.startswith("structure") and abs(d - (100.05 - 99.2 + 0.1 + 0.1)) < 1e-9
    v.m1.swing_lo = 97.0                                          # swing too far: capped at sl_max_atr
    d, src = tr._sl_distance("BUY", m, 0.01)
    assert d == 1.6 and "cap" in src
    v.m1.swing_lo = 99.9                                          # very close swing: floor sl_min_atr
    assert tr._sl_distance("BUY", m, 0.01)[0] == 0.5
    v.m1.swing_lo = 0.0
    assert "no swing" in tr._sl_distance("BUY", m, 0.01)[1]


def test_learned_indicator_weights_persist_and_follow_outcomes(tmp_path):
    p = str(tmp_path / "learn.json")
    lr = Learner(path=p, half_life_h=0)
    for _ in range(20):
        lr.record({"symbol": "BTCUSDm", "bucket": "b", "r": 1.0, "ind_votes": {"ribbon": 1.0, "obv": -1.0}}, 0)
    w = lr.ind_weights("BTCUSDm")
    assert w["ribbon"] > 1.5 and w["obv"] < 0.5
    assert Learner(path=p, half_life_h=0).ind_weights("BTCUSDm") == w        # survives restarts, no decay


def test_new_tunables_validate():
    ok, bad = validate_tune({"sl_max_atr": 2.0, "ind_min_score": 0.3, "ind_chop_veto": 0, "safe_flatten_pct": 70})
    assert not bad and ok["ind_chop_veto"] is False and ok["sl_max_atr"] == 2.0
    c = EngineConfig()
    assert c.mb_cut_frac == 0.0 and c.mb_max_hold_seconds == 0.0 and c.min_orders_per_batch == 2
    assert c.risk_per_batch_pct == 10.0 and c.max_total_risk_pct == 25.0 and c.equity_drawdown_pct == 30.0
