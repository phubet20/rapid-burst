"""t36u: soft exits must not fire on chart glitches / jitter; hard stops are never delayed; entries stay immediate."""
from types import SimpleNamespace as NS

from rapidburst.config import EngineConfig
from tests.harness import make, run
from tests.paths import T0, _small_account, _start, series_up
from tests.test_microburst import _force_signal


def _m(t, mid, **k):
    base = dict(ready=True, mid=mid, t=t, stable=True, gap_max=0.2, latency=0.2, spread_ratio=1.0, atr=1.0)
    base.update(k)
    return NS(**base)


def _tr(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig())
    return tr


def test_single_tick_flicker_never_closes(tmp_path):
    tr = _tr(tmp_path)
    assert tr._confirm_soft_exit("g", "BTCUSDm", "micro exit", _m(100.0, 50.0), 100.0) is None
    assert tr._confirm_soft_exit("g", "BTCUSDm", None, _m(100.2, 50.0), 100.2) is None    # flicker gone
    assert "g" not in tr._exit_pending and tr.exits_filtered == 1


def test_persistent_soft_exit_confirms_after_time_and_ticks(tmp_path):
    tr = _tr(tmp_path)
    out = [tr._confirm_soft_exit("g", "BTCUSDm", "momentum exhausted", _m(100.0 + i * 0.2, 50.0), 100.0 + i * 0.2)
           for i in range(6)]
    assert out[:4] == [None] * 4                              # < 0.8 s
    assert out[5] and "confirmed" in out[5] and "clean ticks" in out[5]


def test_glitch_ticks_do_not_count_as_confirmation(tmp_path):
    tr = _tr(tmp_path)
    for i in range(5):                                        # warm the median with clean mids
        tr._exit_noise("BTCUSDm", _m(90.0 + i * 0.1, 50.0))
    glitches = [_m(101.0, 52.0),                              # outlier tick 2 ATR off the median
                _m(101.2, 50.0, stable=False, gap_max=4.0),   # feed gap
                _m(101.4, 50.0, latency=5.0),                 # stale quote
                _m(101.6, 50.0, spread_ratio=4.0)]            # spread spike
    whys = [tr._exit_noise("BTCUSDm", g) for g in glitches]
    assert all(whys) and "outlier" in whys[0] and "gap" in whys[1] and "stale" in whys[2] and "spread" in whys[3]
    tr2 = _tr(tmp_path / "b")
    for i in range(5):
        tr2._exit_noise("BTCUSDm", _m(90.0 + i * 0.1, 50.0))
    t = 101.0
    for g in glitches * 3:                                    # 2.4 s of glitches only -> never confirmed
        g.t = t
        assert tr2._confirm_soft_exit("g", "BTCUSDm", "REVERSAL: SELL momentum", g, t) is None
        t += 0.2
    assert any(l["msg"].startswith("EXIT HELD") for l in tr2.logs)


def test_engine_ignores_flicker_but_closes_on_persistent_exit_and_hard_stop_is_immediate(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 100.3, T0 + 101)
    held = set(br.positions_)
    assert held
    tr._protect_basket = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    flick = {"n": 0}

    def one_shot(*a, **k):                                    # a glitch: exit signal on ONE loop only
        flick["n"] += 1
        return "spike turning back (glitch)" if flick["n"] == 1 else None
    tr._micro_exit = one_shot
    run(tr, br, T0 + 101.3, T0 + 104)
    assert held <= set(br.positions_)                         # flicker did not close anything
    tr._micro_exit = lambda *a, **k: "momentum exhausted (persistent)"
    run(tr, br, T0 + 104.3, T0 + 106)
    assert not (held & set(br.positions_))
    assert any(l["msg"].startswith("EXIT GROUP") and "confirmed" in l["msg"] for l in tr.logs)
