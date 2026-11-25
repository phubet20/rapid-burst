"""t46u (owner): M1-only fast trend mode + hold winners. SYNTHETIC test data only."""
import os
from types import SimpleNamespace as NS

from rapidburst import microburst as mb, regime
from rapidburst.config import EngineConfig
from rapidburst.mtf import MTFView, TFState
from rapidburst.trader import Trader
from tests.harness import make, run
from tests.paths import T0, _small_account, _start, series_up
from tests.test_microburst import _force_signal
from tests.test_t44u import _fake_micro, _patched_components, _zones


def test_defaults_and_load_force_the_safe_combination(monkeypatch):
    c = EngineConfig()
    assert c.m1_only and c.hold_winners
    monkeypatch.setenv("RB_CONFIG_JSON", '{"no_stop_mode": true, "precision_enabled": true}')
    c = EngineConfig.load()
    assert c.m1_only and not c.precision_enabled and not c.no_stop_mode and c.add_enabled     # always a broker SL
    monkeypatch.setenv("RB_CONFIG_JSON", '{"m1_only": false}')
    c = EngineConfig.load()
    assert not c.m1_only and c.precision_enabled and not c.add_enabled                         # the old precision mode is one switch away


def test_e_family_uses_the_m1_trend_not_m5():
    v = MTFView(m1=TFState(ready=True), m5=TFState(ready=True))
    v.bias, v.m5_bias = 0.6, -0.9
    v.m5_momentum = lambda d: -1.0 * d
    v.timing = lambda d: 0.0
    m = NS(mtf=v)
    _, e, bias = mb._mtf_scores(m, 1, EngineConfig(m1_only=True))
    assert e > 0 and bias == 0.6                                    # M1 trend (EMA / MACD / RSI) decides
    _, e, bias = mb._mtf_scores(m, 1, EngineConfig(m1_only=False))
    assert e < 0 and bias == -0.9                                   # legacy M5 family


def test_m5_regime_veto_is_not_used_in_m1_mode(monkeypatch):
    _patched_components(monkeypatch)
    up = regime.RegimeView(ready=True, state="UP", phase=2, leg="pullback", origin=95.0, atr=1.0)
    legacy = EngineConfig(precision_enabled=False, m1_only=False, no_stop_mode=True)
    fast = EngineConfig(precision_enabled=False, m1_only=True, no_stop_mode=True)
    assert any("against the M5 structure" in x for x in mb.side_strength(_fake_micro(up, _zones()), -1, legacy)[3])
    assert not any("M5" in x for x in mb.side_strength(_fake_micro(up, _zones()), -1, fast)[3])


def _stub(cfg, money, portfolio, prot, bias, rsi, sbreak=0, side="BUY"):
    legs = [NS(ticket=1, symbol="X", side=side)]
    v1 = NS(ready=True, rsi_ready=True, rsi=rsi, sbreak=sbreak)
    m = NS(mtf=NS(m1=v1, bias=bias))
    tr = NS(cfg=cfg, bursts={"k": {"prot": prot}}, _exit_held_log={}, logs=[], exits_filtered=0,
            own_positions=[{"profit": portfolio, "swap": 0.0}], _net=lambda legs, live: money)
    tr.add_log = lambda lvl, msg: tr.logs.append(msg)
    return tr, legs, m


def _hold(cfg, **kw):
    tr, legs, m = _stub(cfg, **kw)
    return Trader._hold_winner(tr, "k", legs, {}, m, "momentum exhausted in profit", 100.0)


def test_hold_winner_rules():
    c = EngineConfig()
    assert _hold(c, money=-1.0, portfolio=-5.0, prot="", bias=0.5, rsi=60) is False        # a loser is never "held" here
    assert _hold(c, money=2.0, portfolio=3.0, prot="", bias=0.5, rsi=60) is True            # winner, M1 trend with it
    assert _hold(c, money=2.0, portfolio=3.0, prot="", bias=-0.5, rsi=40) is False          # trend turned (EMA + RSI): may close
    assert _hold(c, money=2.0, portfolio=3.0, prot="", bias=-0.5, rsi=60) is True           # bias against but RSI not: still held
    assert _hold(c, money=2.0, portfolio=3.0, prot="", bias=0.5, rsi=60, sbreak=-1) is False   # M1 swing broken against it
    assert _hold(c, money=2.0, portfolio=-8.0, prot="BE", bias=-0.5, rsi=40) is True         # portfolio red + stop at BE: hold to the stop
    assert _hold(c, money=2.0, portfolio=-8.0, prot="", bias=-0.5, rsi=40) is False         # no protective stop yet: may leave
    assert _hold(EngineConfig(hold_winners=False), money=2.0, portfolio=3.0, prot="", bias=0.5, rsi=60) is False


def test_sell_side_flip_uses_the_mirrored_rules():
    c = EngineConfig()
    assert _hold(c, money=2.0, portfolio=1.0, prot="", bias=-0.5, rsi=40, side="SELL") is True    # trend down with the SELL
    assert _hold(c, money=2.0, portfolio=1.0, prot="", bias=0.5, rsi=60, side="SELL") is False    # up-trend against the SELL


def test_a_winning_basket_is_not_closed_by_a_soft_exit_while_the_trend_holds(tmp_path):
    def play(hold):
        tr, br = make(tmp_path / f"h{int(hold)}", {"BTCUSDm": series_up()},
                      EngineConfig(precision_enabled=False, m1_only=False, add_enabled=False, hold_winners=hold,
                                   zones_enabled=False, pullback_hold=False, zone_target_exit=False, regime_enabled=False,
                                   exit_confirm_s=0.0, exit_confirm_ticks=1, max_orders_per_batch=2, max_positions_per_symbol=2))
        _start(tr, br, T0 + 1)
        _force_signal(tr, "BUY", origin=T0 + 99.0)
        run(tr, br, T0 + 1, T0 + 3)
        assert br.positions_
        tr._micro_exit = lambda *a, **k: "momentum exhausted in profit (test)"
        tr._reversal_check = lambda *a, **k: None
        tr._net = lambda legs, live: 5.0                                # the basket is in net profit (the sim spread is not)
        run(tr, br, T0 + 3, T0 + 8)
        return tr, br
    closed = lambda tr: [l for l in tr.logs if "EXIT GROUP" in l["msg"] and "momentum exhausted in profit (test)" in l["msg"]]
    tr_hold, br_hold = play(True)
    tr_free, br_free = play(False)
    assert closed(tr_free)                                              # without the rule the soft exit banks the profit
    assert not closed(tr_hold) and br_hold.positions_                   # with the rule the winner is kept (trend with it)
    assert any("HOLD WINNER" in l["msg"] for l in tr_hold.logs)
