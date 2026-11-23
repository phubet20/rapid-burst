"""t37u-t39u owner mode: full batch at once sized by margin, no per-order SL, emergency guards only, zones/FVG/
confirmation/wick inputs, zone-target exit, pullback hold, learner record-only. SYNTHETIC test data."""
from rapidburst import zones
from rapidburst.config import EngineConfig
from tests.harness import make, run
from tests.paths import T0, _small_account, _start, series_up
from tests.test_microburst import _force_signal


def _bar(t, o, h, l, c):
    return {"time": t, "open": o, "high": h, "low": l, "close": c, "tick_volume": 10}


def test_owner_defaults():
    c = EngineConfig()
    assert c.no_stop_mode and c.full_batch and c.max_orders_per_batch == 16 and not c.learning_enabled
    assert c.emergency_margin_level_pct == 150.0 and 0 < c.nostop_margin_use_pct < 100


def test_full_batch_at_once_by_margin_without_sl(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    tr._micro_exit = lambda *a, **k: None
    run(tr, br, T0 + 1, T0 + 3)
    pos = list(br.positions_.values())
    assert len(pos) >= 8, len(pos)                                   # many orders from the first entry
    assert all(not p["sl"] for p in pos)                             # no per-order SL (owner request)
    assert len({p["volume"] for p in pos}) == 1                      # one lot, one point
    margin = sum(p["volume"] for p in pos) * 100.0
    assert margin <= 1000.0 * tr.cfg.nostop_margin_use_pct / 100.0 + 1e-6   # buffer kept
    assert any("no per-order SL" in l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY"))


def test_only_emergency_guards_close_in_no_stop_mode(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    tr._micro_exit = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    run(tr, br, T0 + 1, T0 + 3)
    assert br.positions_
    legs = [ps for ps in tr.pos_state.values() if ps.ticket in br.positions_]
    live = {int(p["ticket"]): p for p in tr.own_positions}
    m, _ = tr._micro("BTCUSDm", T0 + 3)
    assert tr._hard_risk_exit("k", legs, live, m, T0 + 3) is None
    tr.account["margin_level"] = 120.0
    assert "EMERGENCY margin level" in tr._hard_risk_exit("k", legs, live, m, T0 + 3)


def test_zones_fvg_confirmation_and_wick_tip():
    bars = [_bar(i * 300, 100, 100.5, 99.5, 100.1) for i in range(10)]
    bars.append(_bar(3000, 100.0, 100.4, 99.8, 100.2))                # base
    bars.append(_bar(3300, 100.2, 103.5, 100.1, 103.4))               # impulse up
    bars.append(_bar(3600, 103.4, 104.0, 101.0, 103.9))               # leaves a bullish FVG 100.4..101.0
    zs = zones.find_zones(bars, 1.0)
    kinds = {z.kind for z in zs}
    assert "demand" in kinds and "fvg+" in kinds
    assert zones.confirmation([bars[-3], bars[-2]]) == 1
    live = {"open": 100.6, "high": 100.8, "low": 99.2, "close": 100.0}
    assert zones.wick_tip(live, 99.9, 1.0) == 1                       # buy the lower wick tip
    v = zones.analyze(bars, [], live, 100.3, 1.0)
    assert v.ready and v.holds(1) and v.score(1) > v.score(-1)


def test_learner_is_record_only_by_default(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    tr.learner.decide = lambda *a, **k: (_ for _ in ()).throw(AssertionError("learner consulted"))
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 1, T0 + 3)
    assert br.positions_
    assert all("record only" in w for w in tr.learn_view.values())
