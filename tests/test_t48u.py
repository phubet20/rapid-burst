"""t48u (owner): M5 trend + M1 impulse -> pullback entry (no chase, one entry per pullback), loss leaves fast / winner runs,
lot from 40% of equity as margin split into equal orders. SYNTHETIC data: these test the RULES, not market performance."""
from dataclasses import replace as rep
from types import SimpleNamespace as NS

from rapidburst import microburst as mb
from rapidburst.config import EngineConfig, TUNABLE
from rapidburst.mtf import MTFView, TF, ip_eval
from rapidburst.trader import Trader
from tests.harness import make, run
from tests.paths import T0
from tests.test_mtf import _live
from tests.test_t46u import _bar_play

T_0 = 1_800_000_000


def _flat(n=14, base=100.0):
    return [(base, base + 0.5, base - 0.5, base) for _ in range(n)]


def _tf(bars, live_px, now_off=30):
    """closed bars (o,h,l,c) one per minute + a forming bar that has just ticked `live_px`"""
    tf = TF(60)
    closed = [{"time": T_0 + i * 60, "open": o, "high": h, "low": l, "close": c, "tick_volume": 100}
              for i, (o, h, l, c) in enumerate(bars)]
    t_open = T_0 + len(bars) * 60
    tf.load(closed, t_open + 1)
    tf.on_tick(t_open + 1, live_px)
    return tf.state(t_open + now_off)


# up impulse of ~5 ranges in 3 bars (100 -> 105), then a pullback to ~102.8 (about 45%) and a turn back up
UP = _flat() + [(100.0, 101.8, 99.9, 101.7), (101.7, 103.5, 101.6, 103.4), (103.4, 105.0, 103.3, 104.9)]
PB = [(104.9, 105.0, 103.6, 103.8), (103.8, 104.0, 102.8, 103.0)]


def test_defaults_and_tunables():
    c = EngineConfig()
    assert c.ip_enabled and c.alloc_enabled and c.alloc_margin_pct == 40.0 and not c.m1_every_bar
    assert not c.scalp_enabled and not c.sniper_enabled
    for k in ("ip_enabled", "ip_m5_bias_min", "ip_impulse_atr", "ip_retr_min", "ip_retr_max", "ip_fail_retr", "ip_resume_atr",
              "ip_be_atr", "ip_trail_atr", "ip_m5_flip", "alloc_enabled", "alloc_margin_pct"):
        assert k in TUNABLE


def test_impulse_leg_is_measured():
    s = _tf(UP + PB, 103.2)
    ip = s.ip_up
    assert ip["size"] > 3.0 and ip["eff"] > 0.6 and ip["bars"] <= 8
    assert abs(ip["end_px"] - 105.0) < 1e-9 and ip["pb"] <= 102.8 + 1e-9 and 0.3 < ip["retr"] < 0.7
    assert not s.ip_dn or s.ip_dn["size"] < ip["size"]


def test_buy_enters_the_pullback_after_it_turns_with_the_m5_trend():
    cfg = EngineConfig()
    s = _tf(UP + PB, 103.4)                                   # price turned 0.6 up from the pullback low 102.8
    ok, why, info = ip_eval(s, 1, 0.5, cfg)
    assert ok and "IMPULSE-PULLBACK BUY" in why and info["stop"] <= 102.8 + 1e-9 and info["retr"] > 0.3
    assert info["key"] == (T_0 + 13 * 60, T_0 + 17 * 60)          # leg start = the last flat bar's low, end = the (double) top


def test_no_chase_while_the_leg_is_still_running_or_the_pullback_is_shallow():
    cfg = EngineConfig()
    running = _tf(UP, 105.2)                                  # the forming bar is still making new highs: chasing
    ok, why, _ = ip_eval(running, 1, 0.5, cfg)
    assert not ok and "not chasing" in why
    shallow = _tf(UP + [(104.9, 105.0, 104.4, 104.6)], 104.8)  # only ~10% back
    ok, why, _ = ip_eval(shallow, 1, 0.5, cfg)
    assert not ok and "waiting" in why


def test_waits_for_the_turn_and_gives_up_when_the_impulse_fails():
    cfg = EngineConfig()
    falling = _tf(UP + PB, 102.85)                            # at the pullback low, not turned yet
    ok, why, _ = ip_eval(falling, 1, 0.5, cfg)
    assert not ok and "waiting" in why
    deep = UP + [(104.9, 105.0, 102.0, 102.1), (102.1, 102.4, 100.6, 100.8)]
    ok, why, _ = ip_eval(_tf(deep, 101.6), 1, 0.5, cfg)
    assert not ok and "failed" in why


def test_the_m5_trend_must_be_with_the_trade_and_no_impulse_means_no_trade():
    cfg = EngineConfig()
    s = _tf(UP + PB, 103.4)
    assert not ip_eval(s, 1, -0.4, cfg)[0] and "M5 trend" in ip_eval(s, 1, -0.4, cfg)[1]
    assert not ip_eval(s, 1, 0.05, cfg)[0]
    flat = _tf(_flat(20), 100.0)
    ok, why, _ = ip_eval(flat, 1, 0.5, cfg)
    assert not ok and "no impulse" in why


def test_sell_is_the_mirror_image():
    dn = [(2 * 200.0 - o, 2 * 200.0 - l, 2 * 200.0 - h, 2 * 200.0 - c) for (o, h, l, c) in _flat(14, 100.0) + UP[14:] + PB]
    s = _tf(dn, 2 * 200.0 - 103.4)
    ok, why, info = ip_eval(s, -1, -0.5, EngineConfig())
    assert ok and "IMPULSE-PULLBACK SELL" in why and info["stop"] >= 2 * 200.0 - 102.8 - 1e-9


# ------------------------------------------------------------------ decide()
def _view(state, m5_bias):
    v = MTFView(m1=state)
    v.m5 = rep(state)
    v.m5_bias, v.bias, v.supported, v.bias_label = m5_bias, m5_bias, True, "test"
    return v


def test_decide_only_enters_on_the_pullback_and_reports_why_it_waits():
    cfg = EngineConfig(precision_enabled=False)
    st = mb.decide(_live(mtf=_view(_tf(UP + PB, 103.4), 0.5)), cfg)
    assert st.side == "BUY" and st.ip_key and st.ip_stop <= 102.8 + 1e-9 and "IMPULSE-PULLBACK" in st.reason
    st = mb.decide(_live(mtf=_view(_tf(UP, 105.2), 0.5)), cfg)
    assert st.side == "NO TRADE" and "waiting" in st.reason and st.bar_side == ""
    off = mb.decide(_live(mtf=_view(_tf(UP, 105.2), 0.5)), EngineConfig(precision_enabled=False, ip_enabled=False))
    assert not off.ip_key


# ------------------------------------------------------------------ exits: a loss leaves fast, a winner is held
def _ipx(px, stop=100.0, money=-3.0, best=0.0, age=10.0, side="BUY", m5=0.5, prot="", atr=1.0, **kw):
    cfg = EngineConfig(**kw)
    d = 1 if side == "BUY" else -1
    legs = [NS(ticket=1, symbol="X", side=side, volume=0.1, open_price=101.0)]
    g = {"ip": {"stop": stop, "best": best, "t0": 1000.0}, "prot": prot}
    tr = NS(cfg=cfg, bursts={"k": g})
    tr._group_pl = lambda legs, live, m, now: ((px - 101.0) * d, money)
    tr._atr = lambda s: (atr, "M1")
    tr._confirm_soft_exit = lambda key, sym, soft, m, now, confirm_s=None, ticks=None: soft
    m = NS(ready=True, atr=atr, mtf=NS(m5=NS(ready=True), m5_bias=m5))
    return Trader._ip_exit(tr, "k", legs, {}, m, 1000.0 + age), g


def test_impulse_failed_leaves_at_once_when_the_pullback_extreme_is_taken_out():
    r, _ = _ipx(px=99.9, stop=100.0)
    assert r and r.startswith("IMPULSE FAILED")
    assert _ipx(px=100.6, stop=100.0)[0] is None
    assert _ipx(px=99.9, stop=100.0, prot="BE")[0] is None              # a protected winner is the trail's business


def test_m5_turn_and_no_follow_through_cut_the_loss_and_a_good_basket_is_left_alone():
    r, _ = _ipx(px=101.5, money=4.0, m5=-0.5)
    assert r and r.startswith("M5 TREND TURNED")
    assert _ipx(px=101.5, money=4.0, m5=0.1)[0] is None                 # M5 only weakened: keep
    r, _ = _ipx(px=100.8, money=-2.0, age=200.0, best=0.1)
    assert r and r.startswith("NO FOLLOW-THROUGH")
    assert _ipx(px=100.8, money=-2.0, age=60.0, best=0.1)[0] is None    # too early
    assert _ipx(px=100.8, money=-2.0, age=200.0, best=0.5)[0] is None   # it did go 0.5 ATR in profit once
    assert _ipx(px=101.4, money=2.0, age=200.0, best=0.1)[0] is None    # not red


def test_ip_winner_is_held_until_the_m5_trend_turns():
    def hold(m5):
        cfg = EngineConfig()
        legs = [NS(side="BUY", symbol="X")]
        g = {"ip": {"stop": 0.0}, "prot": ""}
        tr = NS(cfg=cfg, bursts={"k": g}, own_positions=[{"profit": 5.0, "swap": 0.0}], exits_filtered=0, _exit_held_log={},
                add_log=lambda *a, **k: None)
        tr._net = lambda legs, live: 5.0
        m = NS(mtf=NS(m1=NS(ready=True, rsi_ready=False, rsi=50.0, sbreak=-1), bias=-0.9, m5=NS(ready=True), m5_bias=m5))
        return Trader._hold_winner(tr, "k", legs, {}, m, "REVERSAL: x", 1.0)
    assert hold(0.4) is True            # M1 even broke (sbreak against) but the M5 trend is still with it -> held
    assert hold(-0.5) is False


def test_profit_protection_is_wide_for_an_impulse_pullback_basket(tmp_path):
    def prot(ip):
        tr, br = _bar_play(tmp_path / f"p{int(ip)}", max_orders_per_batch=2, ladder_enabled=False)
        run(tr, br, T0 + 1, T0 + 3)
        legs = [ps for ps in tr.pos_state.values() if ps.ticket in {int(p["ticket"]) for p in tr.own_positions}]
        key = legs[0].batch_id
        if ip:
            tr.bursts[key]["ip"] = {"stop": 0.0, "best": 0.0, "t0": 0.0}
        m, st = tr._micro("BTCUSDm", T0 + 3)
        tr._atr = lambda sym: (5.0, "M1")
        tr._protect_basket(key, legs, {int(p["ticket"]): p for p in tr.own_positions}, rep(m, ready=True), st, T0 + 3)
        return tr.bursts[key]["prot_trigger"], tr.bursts[key]["trail_dist"]
    t_ip, tr_ip = prot(True)
    t_n, tr_n = prot(False)
    assert t_ip >= 0.9 * 5.0 - 1e-9 and tr_ip >= 1.2 * 5.0 - 1e-9
    assert t_ip > t_n and tr_ip > tr_n


# ------------------------------------------------------------------ sizing from 40% of equity
def _alloc_lots(tmp_path, **cfgkw):
    cfgkw.setdefault("max_orders_per_batch", 4)
    cfgkw.setdefault("min_orders_per_batch", 4)
    tr, br = _bar_play(tmp_path, ladder_enabled=False, max_positions_per_symbol=16, **cfgkw)
    run(tr, br, T0 + 1, T0 + 3)
    vols = [float(p["volume"]) for p in br.positions_.values()]
    eq = tr._acct().equity
    margin = sum(tr.broker.calc_margin("BUY", p["symbol"], float(p["volume"]), float(p["price_open"])) or 0.0
                 for p in br.positions_.values())
    return vols, eq, margin


def test_the_basket_lot_comes_from_the_margin_allocation_and_is_split_equally(tmp_path):
    vols, eq, margin = _alloc_lots(tmp_path / "a", alloc_enabled=True, alloc_margin_pct=40.0, risk_per_batch_pct=100.0,
                                   risk_per_trade_pct=100.0, small_account_batch_risk_pct=100.0, max_margin_use_pct=95.0,
                                   max_total_risk_pct=300.0)
    assert vols and len(set(vols)) == 1                       # equal orders
    assert margin <= eq * 0.40 + 1e-6                          # never more than the allocation
    small, eq2, margin2 = _alloc_lots(tmp_path / "b", alloc_enabled=True, alloc_margin_pct=10.0, risk_per_batch_pct=100.0,
                                      risk_per_trade_pct=100.0, small_account_batch_risk_pct=100.0, max_margin_use_pct=95.0,
                                      max_total_risk_pct=300.0)
    assert margin2 <= eq2 * 0.10 + 1e-6 and max(small) <= max(vols)


def test_the_loss_cap_still_limits_the_allocation(tmp_path):
    vols, eq, margin = _alloc_lots(tmp_path / "c", alloc_enabled=True, alloc_margin_pct=40.0, risk_per_batch_pct=0.5,
                                   risk_per_trade_pct=0.5)
    big, _, _ = _alloc_lots(tmp_path / "d", alloc_enabled=True, alloc_margin_pct=40.0, risk_per_batch_pct=100.0,
                            risk_per_trade_pct=100.0, small_account_batch_risk_pct=100.0, max_margin_use_pct=95.0,
                            max_total_risk_pct=300.0)
    assert max(vols) <= max(big)


# ------------------------------------------------------------------ end to end through the trader
def test_pullback_entry_opens_one_basket_with_the_stop_beyond_the_pullback_and_never_re_enters_it(tmp_path):
    tr, br = _bar_play(tmp_path, ip_enabled=True, m1_every_bar=False, ladder_enabled=False, max_orders_per_batch=2,
                       min_orders_per_batch=1, alloc_enabled=False)
    base = tr._micro

    def ipm(s, now):
        m, st = base(s, now)
        stop = m.bid - 2.0 * (m.atr or 1.0)
        return m, rep(st, side="BUY", buy=0.8, sell=0.0, reason="IMPULSE-PULLBACK BUY (test)", ip_key=(10, 20), ip_stop=stop,
                      ip_info={"start": stop - 1.0, "end": m.ask + 1.0, "retr": 0.45, "key": (10, 20), "stop": stop})
    tr._micro = ipm
    run(tr, br, T0 + 1, T0 + 6)
    gids = [k for k, g in tr.bursts.items() if g.get("ip")]
    assert len(gids) == 1                                           # one entry per pullback (the key is used)
    g = tr.bursts[gids[0]]
    assert "pullback extreme" in g["sl_src"] and g["ip"]["retr"] == 0.45
    assert all(float(p.get("sl", 0.0)) > 0 for p in br.positions_.values())        # broker SL on every order
    assert any("IMPULSE-PULLBACK" in l["msg"] for l in tr.logs if isinstance(l, dict))


def test_an_impulse_basket_has_one_owner_the_old_soft_exits_never_fight_it(tmp_path):
    """t49u: with ip_single_owner the micro/zone/reversal exit families do not close an impulse-pullback basket."""
    def play(owner):
        tr, br = _bar_play(tmp_path / ("own" if owner else "off"), ip_enabled=True, m1_every_bar=False, ladder_enabled=False,
                           max_orders_per_batch=2, min_orders_per_batch=1, alloc_enabled=False, ip_single_owner=owner)
        base = tr._micro

        def ipm(s, now):
            m, st = base(s, now)
            stop = m.bid - 50.0 * (m.atr or 1.0)
            return m, rep(st, side="BUY", buy=0.8, sell=0.0, reason="IMPULSE-PULLBACK BUY (test)", ip_key=(10, 20), ip_stop=stop,
                          ip_info={"start": stop - 1.0, "end": m.ask + 1.0, "retr": 0.45, "key": (10, 20), "stop": stop})
        tr._micro = ipm
        tr._micro_exit = lambda *a, **k: "MICRO test exit"
        tr._confirm_soft_exit = lambda key, sym, soft, m, now: soft
        run(tr, br, T0 + 1, T0 + 8)
        return [l["msg"] for l in tr.logs if isinstance(l, dict) and "MICRO test exit" in l["msg"]]
    assert play(False), "control: the legacy soft exit does close the basket"
    assert not play(True)
