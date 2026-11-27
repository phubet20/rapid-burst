"""t50u: shadow lab (rule hit-rates on the live feed, net of the spread, no orders) + the TP-ladder entry + shadow-only mode.
SYNTHETIC data: these test the MEASURING and the RULES, not market performance."""
from dataclasses import replace as rep
from types import SimpleNamespace as NS

from rapidburst import shadow as sh
from rapidburst.config import EngineConfig, TUNABLE, validate_tune
from rapidburst.shadow import ShadowLab, evaluate
from tests.harness import make, run
from tests.paths import T0, _start, series_up
from tests.test_t46u import _bar_play


def _m(**kw):
    v1 = NS(ready=True, prev_range_x=1.0, prev_lo_wick=0.0, prev_up_wick=0.0, prev_close_pos=0.5, bar_age=7.0)
    base = dict(ready=True, atr=1.0, bid=100.0, ask=100.1, spread=0.1, imbalance=0.0, eff=0.0, seq_consist=0.0, vel=0.0,
                accel=0.0, chase_up=2.0, chase_dn=2.0, rejection=0, mtf=NS(m1=v1))
    base.update(kw)
    return NS(**base)


def _cfg(**kw):
    return EngineConfig(**dict(dict(shadow_confirm_s=6.0, shadow_max_spread_atr=0.5), **kw))


def test_defaults_and_tunables():
    c = EngineConfig()
    assert c.shadow_enabled and c.entries_enabled and not c.tpl_enabled and c.tpl_require_evidence
    assert c.shadow_levels == 5 and abs(c.shadow_step_atr - 0.15) < 1e-9
    for k in ("shadow_enabled", "entries_enabled", "tpl_enabled", "shadow_step_atr", "shadow_levels", "shadow_sl_atr", "tpl_max_hold_s"):
        assert k in TUNABLE
    ok, bad = validate_tune({"entries_enabled": 0, "shadow_levels": 7})
    assert ok == {"entries_enabled": False, "shadow_levels": 7} and not bad


def test_each_rule_reads_its_own_evidence():
    m = _m()
    m.mtf.m1.prev_lo_wick, m.mtf.m1.prev_close_pos = 0.5, 0.8
    assert evaluate(m)["wick"] == 1
    m = _m()
    m.mtf.m1.prev_up_wick, m.mtf.m1.prev_close_pos = 0.5, 0.2
    assert evaluate(m)["wick"] == -1
    assert evaluate(_m(imbalance=-0.5, eff=-0.4, seq_consist=-0.5))["flow"] == -1
    assert evaluate(_m(imbalance=0.5, eff=-0.4, seq_consist=0.5))["flow"] == 0          # disagreement = no opinion
    assert evaluate(_m(vel=0.3, accel=0.1))["mom"] == 1 and evaluate(_m(vel=0.3, accel=-0.1))["mom"] == 0
    m = _m(chase_up=0.2, chase_dn=3.0, rejection=1)
    m.mtf.m1.prev_lo_wick = 0.3
    assert evaluate(m)["loc"] == 1


def test_combo_needs_three_of_four_and_no_opposite():
    m = _m(imbalance=0.5, eff=0.4, seq_consist=0.5, vel=0.3, accel=0.1)
    m.mtf.m1.prev_lo_wick, m.mtf.m1.prev_close_pos = 0.5, 0.8
    assert evaluate(m)["combo"] == 1                                         # wick + flow + mom
    m2 = _m(imbalance=0.5, eff=0.4, seq_consist=0.5, vel=0.3, accel=0.1)
    assert evaluate(m2)["combo"] == 0                                        # only 2 agree
    m3 = _m(imbalance=-0.5, eff=-0.4, seq_consist=-0.5, vel=0.3, accel=0.1)
    m3.mtf.m1.prev_lo_wick, m3.mtf.m1.prev_close_pos = 0.5, 0.8
    assert evaluate(m3)["combo"] == 0                                        # 2 up, 1 down: conflict


def _lab(tmp_path, **kw):
    return ShadowLab(str(tmp_path / "shadow.json"), kw.get("step", 0.15), kw.get("levels", 3), kw.get("sl", 1.0))


def _drive(lab, cfg, prices, m_extra=None, t0=1000.0, dt=1.0, sym="XAU"):
    """one observation point at t0 (confirm point), then follow bid/ask (spread 0.1) once per `dt`"""
    t = t0
    for bid in prices:
        m = _m(bid=bid, ask=bid + 0.1, **(m_extra or {}))
        m.mtf.m1.bar_age = 7.0 + (t - t0)
        lab.feed(sym, m, t, cfg)
        t += dt
    return t


def test_the_lab_scores_net_of_the_spread_and_the_ladder(tmp_path):
    cfg = _cfg()
    lab = _lab(tmp_path, levels=3)
    wick = dict(prev_lo_wick=0.5, prev_close_pos=0.8)
    m0 = _m(bid=100.0, ask=100.1)
    for k, v in wick.items():
        setattr(m0.mtf.m1, k, v)
    m0.mtf.m1.bar_age = 7.0
    lab.feed("XAU", m0, 1000.0, cfg)
    assert lab.pending["XAU"] and {o.rule for o in lab.pending["XAU"]} >= {"wick", "ctrl"}
    o = next(x for x in lab.pending["XAU"] if x.rule == "wick")
    assert o.d == 1 and o.entry == 100.1                                     # a BUY enters at the ASK
    # the bid rises 0.50 (3.3 steps of 0.15): the exit side is the bid, so the net excursion is 100.5-100.1 = 0.4 ATR
    for i, bid in enumerate([100.2, 100.5, 100.5]):
        m = _m(bid=bid, ask=bid + 0.1)
        m.mtf.m1.bar_age = 8.0 + i
        lab.feed("XAU", m, 1001.0 + i, cfg)
    assert abs(o.best - 0.4) < 1e-9 and set(o.hit) == {1, 2}                  # legs at 0.15 and 0.30 ATR, not 0.45
    assert not o.stop_t


def test_the_stop_wins_a_tie_and_unreached_legs_are_a_loss(tmp_path):
    cfg = _cfg()
    lab = _lab(tmp_path, levels=3)
    m0 = _m(bid=100.0, ask=100.1)
    m0.mtf.m1.prev_lo_wick, m0.mtf.m1.prev_close_pos, m0.mtf.m1.bar_age = 0.5, 0.8, 7.0
    lab.feed("XAU", m0, 1000.0, cfg)
    o = next(x for x in lab.pending["XAU"] if x.rule == "wick")
    m = _m(bid=98.9, ask=99.0)                                               # -1.2 ATR net: the stop
    m.mtf.m1.bar_age = 8.0
    lab.feed("XAU", m, 1001.0, cfg)
    assert o.stop_t == 1001.0 and not o.hit
    o.marks = {1: -1.2, 3: -1.2, 5: -1.2}
    assert abs(sh._lad_result(o, 3, 0.15, 1.0) - (-1.0)) < 1e-9


def test_observations_are_booked_after_five_minutes_and_a_gap_drops_them(tmp_path):
    cfg = _cfg()
    lab = _lab(tmp_path, levels=2)
    m0 = _m(bid=100.0, ask=100.1)
    m0.mtf.m1.prev_lo_wick, m0.mtf.m1.prev_close_pos, m0.mtf.m1.bar_age = 0.5, 0.8, 7.0
    lab.feed("XAU", m0, 1000.0, cfg)
    for k in range(1, 31):                                                   # quotes every 10 s for 300 s, bid +0.3 = a steady win
        m = _m(bid=100.3, ask=100.4)
        m.mtf.m1.bar_age = 30.0                                              # outside the confirm window: no new observations
        lab.feed("XAU", m, 1000.0 + 10 * k, cfg)
    r = next(x for x in lab.view(1)["rows"] if x["rule"] == "wick" and x["sym"] == "XAU")
    assert r["n"] == 1 and r["win"]["1"] == 100.0 and r["win"]["5"] == 100.0 and r["hit"][0] == 100.0
    lab2 = _lab(tmp_path / "x" if False else tmp_path, levels=2)
    lab2.stats = {}
    lab2.feed("XAU", m0, 1000.0, cfg)
    m = _m()
    m.mtf.m1.bar_age = 30.0
    lab2.feed("XAU", m, 1100.0, cfg)                                         # 100 s without a quote: dropped, never scored
    assert not lab2.pending["XAU"] and lab2.dropped_stale >= 1 and not lab2.stats


def test_expensive_spread_is_not_an_observation(tmp_path):
    cfg = _cfg(shadow_max_spread_atr=0.05)
    lab = _lab(tmp_path)
    m0 = _m()
    m0.mtf.m1.prev_lo_wick, m0.mtf.m1.prev_close_pos, m0.mtf.m1.bar_age = 0.5, 0.8, 7.0
    lab.feed("XAU", m0, 1000.0, cfg)
    assert not lab.pending.get("XAU") and lab.skipped_cost == 1


def test_one_confirm_point_per_bar_and_fire_once(tmp_path):
    cfg = _cfg()
    lab = _lab(tmp_path)
    m0 = _m(imbalance=0.5, eff=0.4, seq_consist=0.5, vel=0.3, accel=0.1)
    m0.mtf.m1.prev_lo_wick, m0.mtf.m1.prev_close_pos, m0.mtf.m1.bar_age = 0.5, 0.8, 7.0
    lab.feed("XAU", m0, 1000.0, cfg)
    n = len(lab.pending["XAU"])
    m0.mtf.m1.bar_age = 8.0
    lab.feed("XAU", m0, 1001.0, cfg)                                         # same bar: not again
    assert len(lab.pending["XAU"]) == n
    f = lab.fire("XAU", "combo", 90.0, 1001.0)
    assert f and f["d"] == 1
    assert lab.fire("XAU", "combo", 90.0, 1002.0) is None                   # once per bar
    assert lab.fire("XAU", "wick", 90.0, 1500.0) is None                    # too old


def _fake_stat(n, lad_mean, lad_sd):
    import random
    rnd = random.Random(1)
    s = sh.Stat()
    for _ in range(n):
        x = rnd.gauss(lad_mean, lad_sd)
        s.n += 1
        s.lad_sum += x
        s.lad_sq += x * x
    return s


def test_the_verdict_needs_samples_and_a_significant_edge(tmp_path):
    lab = _lab(tmp_path)
    lab.stats["wick|XAU"] = _fake_stat(30, 0.05, 0.2)
    assert lab.row("wick|XAU", 100)["verdict"] == "collecting"
    lab.stats["wick|XAU"] = _fake_stat(400, 0.05, 0.2)
    assert lab.row("wick|XAU", 100)["verdict"] == "positive"
    lab.stats["flow|XAU"] = _fake_stat(400, -0.05, 0.2)
    assert lab.row("flow|XAU", 100)["verdict"] == "negative"
    lab.stats["mom|XAU"] = _fake_stat(400, 0.0, 0.2)
    assert lab.row("mom|XAU", 100)["verdict"] == "no edge"
    assert lab.evidence("XAU", "wick", 100)[0] and not lab.evidence("XAU", "flow", 100)[0]
    assert not lab.evidence("BTC", "nothing", 100)[0]


def test_the_lab_survives_a_restart_but_not_a_changed_ladder(tmp_path):
    lab = _lab(tmp_path, levels=3)
    lab.stats["wick|XAU"] = _fake_stat(50, 0.05, 0.2)
    lab.since = 5.0
    lab.save(force=True)
    again = _lab(tmp_path, levels=3)
    assert again.stats["wick|XAU"].n == 50 and again.since == 5.0
    other = _lab(tmp_path, levels=4)                                          # other ladder = other statistic: start fresh
    assert not other.stats


# ---------------------------------------------------------------- the Trader
def _tpl_play(tmp_path, **cfgkw):
    kw = dict(tpl_enabled=True, tpl_require_evidence=False, ip_enabled=False, m1_every_bar=False, alloc_enabled=False,
              max_orders_per_batch=5, min_orders_per_batch=1, shadow_levels=3, shadow_step_atr=0.15)
    kw.update(cfgkw)
    tr, br = _bar_play(tmp_path, **kw)
    tr._micro = tr.__class__._micro.__get__(tr)                               # the real _micro (+ _tpl_signal), not the test stub
    for eng in tr.engines.values():
        eng.spread_ok = lambda f: (True, "")                                  # the sim's random-walk spread is not under test here
    return tr, br


def test_tpl_waits_for_the_labs_signal_and_never_trades_without_it(tmp_path):
    tr, br = _tpl_play(tmp_path)
    run(tr, br, T0 + 1, T0 + 20)
    assert br.sent_orders == 0
    assert "TP-LADDER waiting" in tr.sym_status["BTCUSDm"]["reason"]


def test_tpl_needs_lab_evidence_by_default(tmp_path):
    tr, br = _tpl_play(tmp_path, tpl_require_evidence=True)
    tr.shadow.fire = lambda *a, **k: {"d": 1, "bar": 1, "atr": 1.0, "spread": 0.0, "rule": "combo"}
    run(tr, br, T0 + 1, T0 + 20)
    assert br.sent_orders == 0
    assert "no positive evidence" in tr.sym_status["BTCUSDm"]["reason"]


def test_tpl_opens_equal_legs_with_a_take_profit_ladder_and_one_shared_stop(tmp_path):
    tr, br = _tpl_play(tmp_path)
    sent = {"n": 0}

    def fire(sym, rule, gap=90.0, now=0.0):
        sent["n"] += 1
        return {"d": 1, "bar": sent["n"], "atr": 1.0, "spread": 0.0, "rule": rule} if sent["n"] == 3 else None
    tr.shadow.fire = fire
    tr._atr = lambda s: (2.0, "M1")
    run(tr, br, T0 + 1, T0 + 8)
    legs = [p for p in br.positions_.values()] + [p for p in []]
    gids = [k for k, g in tr.bursts.items() if g.get("tpl")]
    assert len(gids) == 1
    g = tr.bursts[gids[0]]
    pos = [p for p in list(br.positions_.values())]
    assert len(pos) == 3 or g["legs"] == 3
    tps = sorted(float(p["tp"]) for p in pos)
    sls = {float(p["sl"]) for p in pos}
    assert len(sls) == 1 and all(t > 0 for t in tps)                         # one shared broker stop, a TP on every leg
    gaps = [round(b - a, 6) for a, b in zip(tps, tps[1:])]
    assert gaps and all(abs(x - gaps[0]) < 1e-6 for x in gaps) and gaps[0] >= 0.15 * 2.0 - 1e-6
    assert all(float(p["tp"]) > float(p["price_open"]) for p in pos)         # a BUY's targets sit above the fill
    assert "TP-ladder stop" in g["sl_src"]
    assert any("TP-LADDER" in l["msg"] for l in tr.logs if isinstance(l, dict))


def test_tpl_basket_leaves_by_time_and_is_never_added_to(tmp_path):
    tr, br = _tpl_play(tmp_path, tpl_max_hold_s=30.0)
    sent = {"n": 0}

    def fire(sym, rule, gap=90.0, now=0.0):
        sent["n"] += 1
        return {"d": 1, "bar": sent["n"], "atr": 1.0, "spread": 0.0, "rule": rule} if sent["n"] == 2 else None
    tr.shadow.fire = fire
    tr._atr = lambda s: (2.0, "M1")
    run(tr, br, T0 + 1, T0 + 6)
    gids = [k for k, g in tr.bursts.items() if g.get("tpl")]
    assert len(gids) == 1
    first = tr.bursts[gids[0]]["tpl"]["t0"]
    n_orders = br.sent_orders
    tr.bursts[gids[0]]["tpl"]["t0"] = first - 100.0                           # the lab's window has ended
    run(tr, br, T0 + 7, T0 + 12)
    assert br.sent_orders == n_orders                                         # no add, no new basket from the same signal
    assert not br.positions_ or all(p["symbol"] != "BTCUSDm" for p in br.positions_.values())
    assert any("TP-LADDER TIME" in l["msg"] for l in tr.logs if isinstance(l, dict))


def test_shadow_only_mode_runs_the_lab_and_never_opens_anything(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig(entries_enabled=False, shadow_max_spread_atr=100.0,
                                                                     shadow_confirm_s=0.0))
    _start(tr, br, T0 + 1)
    assert tr.running
    run(tr, br, T0 + 1, T0 + 200)
    assert br.sent_orders == 0 and not tr.own_positions
    assert "SHADOW ONLY" in tr.sym_status["BTCUSDm"]["reason"]
    assert tr.get_snapshot() is not None


def test_snapshot_carries_the_lab_table(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, EngineConfig(entries_enabled=False))
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 5)
    tr._build_snapshot(T0 + 5)
    snap = tr.get_snapshot()
    assert "shadow" in snap and snap["shadow"]["levels"] == 5 and "rows" in snap["shadow"]
