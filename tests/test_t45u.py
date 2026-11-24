"""t45u Structure-to-Liquidity precision entry. SYNTHETIC candles and the simulated broker - these tests check the LOGIC
(structure, OB, liquidity, fibo as confluence, location vs direction, structural stop, R:R, target / invalidation exits).
They are NOT market results and say nothing about profitability."""
import math
import os
from types import SimpleNamespace

from rapidburst import microburst as mb
from rapidburst import precision
from rapidburst.config import EngineConfig
from rapidburst.microburst import Micro, decide
from tests.harness import make, run
from tests.paths import T0, _small_account, _start, series_up
from tests.test_t34u import _controlled


def _bar(i, o, h, l, c):
    return {"time": i * 300.0, "open": o, "high": h, "low": l, "close": c, "vol": 100.0}


def _walk(bars, closes, wick=0.3):
    px = bars[-1]["close"] if bars else closes[0]
    for c in closes:
        i = len(bars)
        bars.append(_bar(i, px, max(px, c) + wick, min(px, c) - wick, c))
        px = c
    return bars


def up_setup(pullback_to=102.7, mirror=False):
    """~60 M5 bars: a range around 100, a displacement up through the range high (BOS) with a bearish candle (the order
    block) right before the biggest thrust candle, an extreme near 107.6, then a pullback to `pullback_to`."""
    bars = []
    rng = [100 + 0.8 * math.sin(k * 2 * math.pi / 8) for k in range(1, 41)]
    _walk(bars, rng)
    _walk(bars, [100.9, 101.8, 102.8])                                   # first thrust leg: BOS above the range high
    bars.append(_bar(len(bars), 103.0, 103.1, 102.4, 102.6))              # bearish candle = the bullish OB (102.4-103.0)
    bars.append(_bar(len(bars), 102.6, 106.0, 102.5, 105.8))              # displacement candle (largest body)
    bars.append(_bar(len(bars), 105.8, 107.6, 105.6, 107.4))              # impulse extreme 107.6
    _walk(bars, [106.4, 105.4, 104.4, 103.6, pullback_to + 0.2, pullback_to], wick=0.15)
    if mirror:                                                           # the same picture, inverted around 200
        for b in bars:
            b["open"], b["close"] = 200 - b["open"], 200 - b["close"]
            b["high"], b["low"] = 200 - b["low"], 200 - b["high"]
    return bars


def _m(mid, spread=0.1, **kw):
    base = dict(ask=mid + spread / 2, bid=mid - spread / 2, mid=mid, spread=spread, atr=1.0, rejection=0, struct_break=0,
                breakout=0, failed=0, rebound=0, vel=0.0, accel=0.0, act_ratio=1.0, mtf=None, ind=None, zones=None)
    base.update(kw)
    return SimpleNamespace(**base)


def _view(bars, cfg=None):
    cfg = cfg or EngineConfig()
    return precision.Analyzer().view("T", bars, None, cfg), cfg


def test_core_finds_break_displacement_order_block_and_liquidity():
    v, _ = _view(up_setup())
    c = v.core
    assert c.ready and c.event.dir == 1 and c.event.kind in ("BOS", "CHOCH") and c.disp_ok
    assert 107.5 <= c.extreme <= 107.8 and c.event.origin_px < 100.2
    assert c.ob is not None and c.ob.valid and abs(c.ob.lo - 102.4) < 0.01 and abs(c.ob.hi - 103.0) < 0.01
    kinds = {k for _, k, _ in c.liq}
    assert "swing high" in kinds and "range high (previous high)" in kinds
    assert v.brief()["ready"] and v.brief()["dir"] == 1


def test_pullback_into_ob_plus_fibo_with_a_trigger_is_an_entry_with_structural_stop_and_target():
    v, cfg = _view(up_setup())
    p = precision.evaluate(v, _m(102.8, rejection=1, vel=0.2), 1, cfg)
    assert p.ready and not p.vetoes, p.vetoes
    assert "fibo" in p.tags and "OB" in p.tags and "rejection" in p.tags
    assert p.ok(cfg) and 0.4 <= p.depth <= 0.7
    assert p.sl < 102.4 and p.inval <= 102.45                          # beyond the pullback low / OB, not a fixed distance
    assert p.target >= 107.0 and p.target_kind and p.rr >= cfg.pe_rr_min
    assert p.key and p.kind


def test_fibo_alone_is_not_an_entry():
    v, cfg = _view(up_setup())
    # same pullback, but the OB is not part of the picture: no OB, no S/R, no sweep, no zone -> only the fibo
    v.core.ob = None
    p = precision.evaluate(v, _m(102.8, rejection=1), 1, cfg)
    assert any("no confluence" in x for x in p.vetoes) and not p.ok(cfg)


def test_chasing_is_refused_price_must_be_in_the_pullback_zone():
    v, cfg = _view(up_setup(pullback_to=106.9))
    p = precision.evaluate(v, _m(107.0, rejection=1, vel=0.3), 1, cfg)
    assert any("not in the pullback zone" in x for x in p.vetoes)
    v2, cfg = _view(up_setup())
    bounced = precision.evaluate(v2, _m(104.9, rejection=1, vel=0.3), 1, cfg)      # bounced far out of the zone
    assert any("not in the pullback zone" in x for x in bounced.vetoes)


def test_no_entry_against_the_structure_and_none_when_the_structure_is_lost():
    v, cfg = _view(up_setup())
    sell = precision.evaluate(v, _m(102.8, rejection=-1), -1, cfg)
    assert any("M5 structure is UP" in x for x in sell.vetoes)
    lost = precision.evaluate(v, _m(98.5, rejection=1), 1, cfg)
    assert any("structure lost" in x for x in lost.vetoes)


def test_waits_for_a_trigger_stop_is_not_widened_and_reward_to_risk_is_required():
    v, cfg = _view(up_setup())
    wait = precision.evaluate(v, _m(102.8, vel=-0.2), 1, cfg)
    assert any("waiting for the trigger" in x for x in wait.vetoes) and wait.waiting
    wide = precision.evaluate(v, _m(102.8, rejection=1), 1, EngineConfig(pe_max_sl_atr=0.1))
    assert any("too wide" in x for x in wide.vetoes)
    poor = precision.evaluate(v, _m(102.8, rejection=1), 1, EngineConfig(pe_rr_min=60.0))
    assert any("no liquidity target" in x for x in poor.vetoes)
    costly = precision.evaluate(v, _m(102.8, spread=0.9, rejection=1), 1, cfg)
    assert any("spread" in x for x in costly.vetoes)


def test_mirror_sell_setup_is_symmetric():
    v, cfg = _view(up_setup(mirror=True))
    assert v.core.event.dir == -1
    p = precision.evaluate(v, _m(200 - 102.8, rejection=-1, vel=-0.2), -1, cfg)
    assert not p.vetoes and p.sl > 200 - 102.4 and p.target < 200 - 107.0 and p.rr >= cfg.pe_rr_min


def test_a_liquidity_sweep_of_a_swing_low_is_a_confluence_factor():
    bars = up_setup()
    sw = min(b["low"] for b in bars[30:40])
    # the last pullback bar wicks under an older swing low and closes back above it
    bars[-1] = _bar(len(bars) - 1, 102.9, 103.0, sw - 0.5, sw + 0.4)
    c = precision.analyze_core(bars, EngineConfig())
    assert any(s[0] == 1 for s in c.sweeps) and len(c.sweeps) == len(set(c.sweeps))


def test_direction_alone_is_not_enough_location_decides():
    """Right direction (momentum passes) but a poor location -> NO TRADE with the reason; right location -> trade."""
    cfg = EngineConfig()
    real = mb.side_strength

    def strong(m, d, c):
        return (0.9, {}, (0.7, 0.7, 0.7, 0.7, 0.7, 0.7), []) if d > 0 else (0.1, {}, (0.1,) * 6, [])
    mb.side_strength = strong
    try:
        v, _ = _view(up_setup())

        def snap(mid, **kw):
            m = Micro(ready=True, stable=True, regime="normal", spread=0.1, atr=1.0, vu=0.1, target=0.3, mid=mid,
                      ask=mid + 0.05, bid=mid - 0.05, prec=v, **kw)
            return m
        chase = decide(snap(107.0, rejection=1, vel=0.3), cfg)
        assert chase.side == "NO TRADE" and "location rejected" in chase.reason and chase.buy > chase.threshold
        good = decide(snap(102.8, rejection=1, vel=0.3), cfg)
        assert good.side == "BUY" and "PE " in good.reason and good.plans["BUY"].ok(cfg)
        assert good.pe["BUY"] > 0.5                                   # separate from the momentum score
        none = decide(Micro(ready=True, stable=True, regime="normal", spread=0.1, atr=1.0, vu=0.1, target=0.3), cfg)
        assert none.side == "NO TRADE"                                # no structure view = no entry
    finally:
        mb.side_strength = real


def test_defaults_stop_loss_on_and_never_forced_off_by_config():
    c = EngineConfig()
    assert c.precision_enabled and not c.no_stop_mode and c.broker_sl and not c.add_enabled
    os.environ["RB_CONFIG_JSON"] = '{"no_stop_mode": true, "hedge_enabled": true}'
    try:
        c = EngineConfig.load()
    finally:
        os.environ.pop("RB_CONFIG_JSON")
    assert not c.no_stop_mode and not c.hedge_enabled and not c.avg_enabled


# ------------------------------------------------------------------ trader integration (simulated broker)
def _session(tmp_path, **kw):
    cfg = EngineConfig(**dict(dict(precision_enabled=True, no_stop_mode=False, full_batch=False, max_orders_per_batch=4,
                                   max_positions_per_symbol=8, zones_enabled=False, pullback_hold=False,
                                   zone_target_exit=False, regime_enabled=False, hedge_enabled=False, avg_enabled=False,
                                   add_enabled=False, learning_enabled=False, risk_per_trade_pct=0.5,
                                   risk_per_batch_pct=1.0, small_account_batch_risk_pct=10.0, max_total_risk_pct=5.0,
                                   max_total_lots=5.0, max_batch_lots=2.0, max_lot_per_order=1.0, group_loss_pct=5.0,
                                   equity_drawdown_pct=30.0, pe_max_sl_atr=1000.0), **kw))
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    ctl = _controlled(tr)
    real = tr._micro
    box = {"sl_gap": 1.0, "tp_gap": 4.0, "key": "k1", "tags": ["fibo", "OB"]}

    def with_plan(s, now_wall):
        m, st = real(s, now_wall)
        if m.ready and st.side in ("BUY", "SELL"):
            d = 1 if st.side == "BUY" else -1
            ref = m.ask if d > 0 else m.bid
            st.plans = {st.side: precision.Plan(st.side, ready=True, score=0.8, tags=box["tags"], key=box["key"], kind="BOS",
                                                entry=ref, sl=ref - d * box["sl_gap"], inval=ref - d * (box["sl_gap"] - 0.2),
                                                target=ref + d * box["tp_gap"], target_kind="swing high", rr=4.0,
                                                risk=box["sl_gap"], depth=0.6, zone=(ref - 1, ref + 1))}
        return m, st
    tr._micro = with_plan
    tr._micro_exit = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    return tr, br, ctl, box


def test_batch_uses_the_structural_stop_and_records_the_plan(tmp_path):
    tr, br, ctl, box = _session(tmp_path)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    assert br.positions_
    for p in br.positions_.values():
        assert p["sl"] and p["price_open"] - p["sl"] > 0.9                    # the structural stop (>= the 1.0 plan gap - spread)
    g = next(iter(tr.bursts.values()))
    assert g["plan"]["tags"] == ["fibo", "OB"] and g["plan"]["target"] > g["plan"]["entry"]
    assert any(l["msg"].startswith("PRECISION") for l in tr.logs)


def test_one_entry_per_structure_event(tmp_path):
    tr, br, ctl, box = _session(tmp_path)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    first = set(br.positions_)
    assert first
    for t in list(first):                                                      # the basket closes (any way)
        br.close_position(t, 50, tr.cfg.magic)
    tr._adopt_positions()
    run(tr, br, T0 + 3.3, T0 + 6)
    assert not [t for t in br.positions_ if t not in first]                    # same key: not entered again
    assert any("already entered" in str(tr.sym_status["BTCUSDm"].get("reason", "")) for _ in [0])
    box["key"] = "k2"                                                          # a NEW break of structure = a new setup
    run(tr, br, T0 + 6.3, T0 + 9)
    assert [t for t in br.positions_ if t not in first]


def test_reward_to_risk_is_rechecked_on_the_live_price_before_sending(tmp_path):
    tr, br, ctl, box = _session(tmp_path)
    box["tp_gap"] = 0.8                                                        # target only 0.8 away vs a 1.0 stop
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    assert not br.positions_
    assert any("reward-to-risk" in str(l["msg"]) for l in tr.logs) or "reward-to-risk" in str(tr.sym_status["BTCUSDm"])


def test_target_liq_is_taken_and_invalidation_exits_without_waiting_for_the_stop(tmp_path):
    tr, br, ctl, box = _session(tmp_path)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    held = set(br.positions_)
    assert held
    ctl["side"] = "NO TRADE"
    g = next(iter(tr.bursts.values()))
    g["plan"]["target"] = 1e9                                                  # nothing reached: held, no cents-level exit
    g["plan"]["inval"] = -1e9
    tr._net = lambda legs, live: 0.05
    run(tr, br, T0 + 3.3, T0 + 5)
    assert held <= set(br.positions_)
    g["plan"]["target"] = 0.0                                                  # price is above the target -> take it
    run(tr, br, T0 + 5.3, T0 + 6.5)
    assert not (held & set(br.positions_))
    assert any("TARGET LIQ reached" in l["msg"] for l in tr.logs if l["msg"].startswith("EXIT GROUP"))
    # a new basket whose invalidation is taken out leaves at once (after the anti-jitter confirmation)
    box["key"] = "k3"
    ctl["side"] = "BUY"
    run(tr, br, T0 + 6.8, T0 + 8)
    held2 = set(br.positions_)
    assert held2
    ctl["side"] = "NO TRADE"
    g2 = next(iter(tr.bursts.values()))
    g2["plan"]["target"] = 1e9
    g2["plan"]["inval"] = 1e9                                                  # price is below the invalidation level
    run(tr, br, T0 + 8.3, T0 + 12)
    assert not (held2 & set(br.positions_))
    assert any("STRUCTURE INVALIDATED" in l["msg"] for l in tr.logs if l["msg"].startswith("EXIT GROUP"))


def test_precision_report_has_the_spec_metrics_from_real_closed_rows(tmp_path):
    from rapidburst.trader import Trader
    rows = [{"pl": 5.0, "risk": 2.0, "exit": "TARGET LIQ reached: swing high", "precision": {"risk": 1.0, "mae": 0.3, "mfe": 3.0, "rr": 3.0, "depth": 0.6, "pe": 0.7, "tags": ["fibo", "OB"]}},
            {"pl": -2.0, "risk": 2.0, "exit": "STRUCTURE INVALIDATED: x", "precision": {"risk": 1.0, "mae": 1.0, "mfe": 0.2, "rr": 2.0, "depth": 0.5, "pe": 0.6, "tags": ["fibo", "S/R"]}},
            {"pl": 3.0, "risk": 1.0, "exit": "x"}]
    rep = Trader._precision_report(rows, [r["pl"] for r in rows])
    assert rep["baskets"] == 2 and rep["win_rate"] == 50.0 and rep["net_after_costs"] == 3.0
    assert rep["profit_factor"] == 2.5 and rep["target_liq_reached_pct"] == 50.0 and rep["invalidation_exit_pct"] == 50.0
    assert rep["avg_mae_R"] == 0.65 and rep["avg_mfe_R"] == 1.6 and rep["by_confluence"]["fibo"]["n"] == 2
