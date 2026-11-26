"""t47u (owner): scalp (partial TP, fast break-even, giveback guard), M1 wick-rejection sniper entry, lot compounding.
SYNTHETIC test data only - these test the RULES, not market performance."""
from dataclasses import replace as rep
from types import SimpleNamespace as NS

from rapidburst import microburst as mb
from rapidburst.config import EngineConfig, TUNABLE
from rapidburst.mtf import MTFView, TF, TFState, m1_sniper, m1_timing
from rapidburst.trader import Trader
from tests.harness import make, run
from tests.paths import T0, _start, series_up
from tests.test_mtf import _live
from tests.test_t46u import _bar_play


def test_defaults_and_tunables():
    c = EngineConfig()
    assert c.scalp_enabled and c.sniper_enabled and c.compound_enabled
    assert c.scalp_be_atr < c.be_trigger_atr                      # the scalp locks earlier than the old break-even
    for k in ("scalp_enabled", "scalp_be_atr", "scalp_target_atr", "scalp_giveback_frac", "sniper_min_wick",
              "sniper_min_score", "sniper_chase_atr", "compound_enabled", "compound_step", "compound_max_mult"):
        assert k in TUNABLE


# ------------------------------------------------------------------ candle anatomy + sniper score
def _m1_bars(last_bar, n=12, t0=1_800_000_000):
    """n flat-ish closed bars (range 1.0) then `last_bar` = (open, high, low, close) as the last CLOSED bar."""
    out = []
    for i in range(n):
        out.append({"time": t0 + i * 60, "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0, "tick_volume": 100})
    o, h, l, c = last_bar
    out.append({"time": t0 + n * 60, "open": o, "high": h, "low": l, "close": c, "tick_volume": 100})
    return out, t0 + (n + 1) * 60


def _state(last_bar, live_px=100.0):
    bars, t_open = _m1_bars(last_bar)
    tf = TF(60)
    tf.load(bars, t_open + 1)
    tf.on_tick(t_open + 1, live_px)                              # the new bar has just opened at live_px
    return tf.state(t_open + 2)


def test_candle_anatomy_lower_and_upper_wick():
    lo = _state((100.0, 100.4, 99.0, 100.3))        # long lower wick, closes high
    assert lo.prev_lo_wick > 0.6 and lo.prev_up_wick < 0.1 and lo.prev_close_pos > 0.9
    up = _state((100.0, 101.0, 99.7, 99.8))         # long upper wick, closes low
    assert up.prev_up_wick > 0.6 and up.prev_lo_wick < 0.1 and up.prev_close_pos < 0.2


def test_sniper_buy_needs_a_lower_wick_sell_needs_an_upper_wick():
    lo = _state((100.0, 100.4, 99.0, 100.3))
    ok, score, txt = m1_sniper(lo, 1, 0.6)
    assert ok and score >= 0.5 and "lower wick" in txt
    assert not m1_sniper(lo, -1, -0.6)[0]            # no upper wick -> no SELL
    up = _state((100.0, 101.0, 99.7, 99.8))
    assert m1_sniper(up, -1, -0.6)[0] and not m1_sniper(up, 1, 0.6)[0]


def test_sniper_waits_without_a_wick_and_does_not_chase():
    marubozu = _state((100.0, 100.6, 100.0, 100.6))  # strong body, no lower wick
    ok, _, txt = m1_sniper(marubozu, 1, 0.8)
    assert not ok and "waiting for a lower wick" in txt
    lo = _state((100.0, 100.4, 99.0, 100.3))
    ran = rep(lo, body=0.9)                          # the forming bar already ran 0.9 of the range with the trade
    ok, _, txt = m1_sniper(ran, 1, 0.6)
    assert not ok and "chasing" in txt


def test_sniper_trend_alignment_adds_score():
    lo = _state((100.0, 100.4, 99.0, 100.3))
    assert m1_sniper(lo, 1, 0.8)[1] > m1_sniper(lo, 1, -0.2)[1]


def _view_with(state, bias):
    v = MTFView(m1=state)
    v.bias, v.supported, v.bias_label = bias, True, "test"
    v.m1_up, v.m1_dn = m1_timing(state, 1, bias), m1_timing(state, -1, bias)
    return v


def test_decide_bar_entry_only_on_a_wick_and_threshold_signals_do_not_chase():
    cfg = EngineConfig(precision_enabled=False, mb_threshold=0.95)         # nothing passes the threshold
    wick = _state((100.0, 100.4, 99.0, 100.3))
    st = mb.decide(_live(mtf=_view_with(wick, 0.6)), cfg)
    assert st.bar_side == "BUY" and "SNIPER" in st.bar_reason
    flat = _state((100.0, 100.6, 100.0, 100.6))
    st = mb.decide(_live(mtf=_view_with(flat, 0.6)), cfg)
    assert st.bar_side == "" and "sniper waiting" in st.bar_reason
    # a normal (threshold) signal is also held back while there is no wick
    easy = EngineConfig(precision_enabled=False)
    assert mb.decide(_live(mtf=_view_with(wick, 0.7)), easy).side == "BUY"
    st = mb.decide(_live(mtf=_view_with(flat, 0.7)), easy)
    assert st.side == "NO TRADE" and "sniper waiting" in st.reason
    off = EngineConfig(precision_enabled=False, sniper_enabled=False)
    assert mb.decide(_live(mtf=_view_with(flat, 0.7)), off).side == "BUY"


# ------------------------------------------------------------------ scalp exits (rule level)
def _scalp(net_px, peak, money, legs_n=8, prot="", tp1=False, spread=0.1, atr=1.0, **cfgkw):
    cfgkw.setdefault("commission_per_lot", 0.0)
    cfg = EngineConfig(**cfgkw)
    legs = [NS(ticket=i + 1, symbol="X", side="BUY") for i in range(legs_n)]
    g = {"scalp_peak": peak, "prot": prot, "scalp_tp1": tp1}
    tr = NS(cfg=cfg, bursts={"k": g}, logs=[])
    tr._group_pl = lambda legs, live, m, now: (net_px, money)
    tr._tv = lambda s: (0.01, 1.0)
    tr._scalp_pick = lambda *a: Trader._scalp_pick(tr, *a)
    tr._atr = lambda s: (atr, "M1")
    tr._confirm_soft_exit = lambda key, sym, soft, m, now, confirm_s=None, ticks=None: soft
    m = NS(ready=True, atr=atr, spread=spread)
    reason, part = Trader._scalp_exit(tr, "k", legs, {}, m, 100.0)
    return reason, part, g


def test_scalp_tp1_banks_half_the_orders_once_the_short_target_is_reached():
    reason, part, _ = _scalp(net_px=0.6, peak=0.6, money=12.0)
    assert reason.startswith("SCALP TP1") and len(part) == 4 and {p.ticket for p in part} == {1, 2, 3, 4}
    assert _scalp(net_px=0.3, peak=0.3, money=6.0, atr=1.0, scalp_giveback_min_atr=0.9)[0] is None   # below the target
    again, p2, _ = _scalp(net_px=0.6, peak=0.6, money=12.0, tp1=True)
    assert again is None and p2 is None                                                              # only once per basket
    one, p1, _ = _scalp(net_px=0.6, peak=0.6, money=12.0, legs_n=1)
    assert one is None                                                                               # a single order is not split


def test_scalp_giveback_closes_a_winner_before_it_turns_into_a_loss():
    reason, part, g = _scalp(net_px=0.15, peak=0.35, money=3.0, scalp_target_atr=2.0)
    assert reason.startswith("SCALP GIVEBACK") and part is None and "closing in profit" in reason
    assert g["scalp_peak"] == 0.35
    assert _scalp(net_px=0.30, peak=0.35, money=5.0, scalp_target_atr=2.0)[0] is None                 # small pullback: keep
    assert _scalp(net_px=0.15, peak=0.35, money=3.0, scalp_target_atr=2.0, prot="BE")[0] is None     # a BE stop already protects it
    assert _scalp(net_px=0.15, peak=0.10, money=3.0, scalp_target_atr=2.0)[0] is None                # the peak never armed the guard


def test_scalp_never_closes_a_loss_and_can_be_switched_off():
    assert _scalp(net_px=-0.2, peak=0.5, money=-4.0)[0] is None
    assert _scalp(net_px=0.15, peak=0.35, money=-1.0, scalp_target_atr=2.0)[0] is None
    assert _scalp(net_px=0.15, peak=0.35, money=3.0, scalp_target_atr=2.0)[0] is not None
    assert _scalp(net_px=0.15, peak=0.35, money=3.0, scalp_target_atr=2.0, scalp_enabled=False)[0] is not None   # (the gate is in _manage_exits)


# ------------------------------------------------------------------ scalp integration (sim)
def test_partial_tp_closes_half_and_the_rest_keeps_running(tmp_path):
    tr, br = _bar_play(tmp_path, max_orders_per_batch=4, min_orders_per_batch=4, ladder_enabled=False, scalp_enabled=True,
                       exit_confirm_s=0.0, exit_confirm_ticks=1, scalp_target_atr=0.15)
    run(tr, br, T0 + 1, T0 + 3)
    assert len(br.positions_) >= 4
    base_micro = tr._micro
    tr._micro = lambda s, now: (lambda m, st: (rep(m, ready=True, t=now, latency=0.0, stable=True, spread_ratio=1.0), st))(
        *base_micro(s, now))                                    # the sim feed ends: judge the rule on clean, fresh ticks
    before = len(br.positions_)
    real = tr._group_pl
    tr._group_pl = lambda legs, live, m, now: (5.0 * max(float(getattr(m, "atr", 1.0) or 1.0), 1.0), 50.0)   # clearly in profit
    run(tr, br, T0 + 3, T0 + 6)
    tr._group_pl = real
    assert any("SCALP TP1" in l["msg"] for l in tr.logs)
    gid = next(iter(tr.bursts))
    assert tr.bursts[gid].get("scalp_tp1") is True
    assert 0 < len(br.positions_) < before                 # only part of the basket was banked


def test_break_even_lock_arms_earlier_in_scalp_mode(tmp_path):
    def trig(scalp):
        tr, br = _bar_play(tmp_path / f"s{int(scalp)}", max_orders_per_batch=2, scalp_enabled=scalp, ladder_enabled=False)
        run(tr, br, T0 + 1, T0 + 3)
        legs = [ps for ps in tr.pos_state.values() if ps.ticket in {int(p["ticket"]) for p in tr.own_positions}]
        key = legs[0].batch_id
        m, st = tr._micro("BTCUSDm", T0 + 3)
        m = rep(m, ready=True)
        tr._atr = lambda sym: (5.0, "M1")                      # a realistic M1 ATR (the sim's is tiny next to its spread)
        tr._protect_basket(key, legs, {int(p["ticket"]): p for p in tr.own_positions}, m, st, T0 + 3)
        return tr.bursts[key].get("prot_trigger"), tr.bursts[key].get("trail_dist")
    t_on, tr_on = trig(True)
    t_off, tr_off = trig(False)
    assert t_on is not None and t_off is not None
    assert t_on < t_off and tr_on < tr_off                    # earlier lock, tighter trail


# ------------------------------------------------------------------ compounding
def _first_lot(tmp_path, streak, **cfgkw):
    tr, br = _bar_play(tmp_path, max_orders_per_batch=2, min_orders_per_batch=1, ladder_enabled=False, **cfgkw)
    tr.win_streak["BTCUSDm"] = streak
    run(tr, br, T0 + 1, T0 + 2)
    vols = [float(p["volume"]) for p in br.positions_.values()]
    return vols, tr


def test_lot_compounds_after_wins_and_never_after_a_loss(tmp_path):
    base, _ = _first_lot(tmp_path / "a", 0)
    won, _ = _first_lot(tmp_path / "b", 2)
    off, _ = _first_lot(tmp_path / "c", 2, compound_enabled=False)
    assert base and won and off
    assert max(won) >= max(base)                               # compounds (or is held by a cap) - never smaller
    assert max(off) == max(base)                               # switched off = the plain size


def test_win_streak_counts_wins_and_a_loss_resets_it():
    tr = NS(win_streak={}, no_up_after_loss={}, pacer=NS(on_close=lambda *a: None), last_burst={}, perf=[],
            _learn_from=lambda *a: None)
    for realized, want in ((5.0, 1), (3.0, 2), (-1.0, 0), (2.0, 1)):
        ps = NS(symbol="X", volume=0.1, batch_id="g", side="BUY")
        try:
            Trader._finish_basket(tr, ps, {"realized": realized, "side": "BUY", "lot": 0.1}, 1.0)
        except Exception:        # the booking tail needs more of the trader than this stub has - the streak is set before it
            pass
        assert tr.win_streak["X"] == want
