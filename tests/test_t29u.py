"""Immediate Continuous Score Momentum Scalper (t29u): dual-side decide, score-driven small-account baskets,
pending real P/L for broker-side closes, dup guard only on a failed signal, runtime tuning without a redeploy.
SimBroker + SYNTHETIC ticks only (test double, not market data)."""
import json
import math

import rapidburst.microburst as mb
from rapidburst.config import TUNABLE, EngineConfig, validate_tune
from rapidburst.microburst import Micro, decide
from tests.harness import conservative, make, run
from tests.paths import T0, _force_signal, _small_account, _start, series_up


def _decide(buy, sell, fam_b, fam_s, veto_b=(), veto_s=()):
    real = mb.side_strength

    def fake(m, side, cfg_):
        return (buy, {}, fam_b, list(veto_b)) if side > 0 else (sell, {}, fam_s, list(veto_s))
    mb.side_strength = fake
    try:
        return decide(Micro(ready=True, stable=True, regime="fast", spread=0.1, atr=1.0, vu=0.1, target=0.3),
                      EngineConfig())
    finally:
        mb.side_strength = real


def test_dual_side_scores_each_side_on_its_own_evidence():
    weak = (0.1, 0.1, 0.1, 0.1, 0.1)
    good = (0.6, 0.2, 0.7, 0.6, 0.2)
    assert _decide(0.2, 0.85, weak, good).side == "SELL"
    assert _decide(0.85, 0.2, good, weak).side == "BUY"
    st = _decide(0.9, 0.1, good, weak, veto_b=["chasing"])        # vetoed BUY never turns into a SELL
    assert st.side == "NO TRADE" and "chasing" in st.reason and "SELL" in st.reason
    assert _decide(0.8, 0.78, good, good).side == "NO TRADE"       # no edge between the sides


def test_validate_tune_is_a_bounded_whitelist():
    ok, bad = validate_tune({"mb_threshold": 0.6, "max_orders_per_batch": 4.0, "live": 1, "magic": 1,
                             "equity_drawdown_pct": 99, "mb_edge": float("nan"), "learning_enabled": 0})
    assert ok == {"mb_threshold": 0.6, "max_orders_per_batch": 4, "learning_enabled": False}
    assert isinstance(ok["max_orders_per_batch"], int)
    assert {b.split(":")[0] for b in bad} >= {"live", "magic", "equity_drawdown_pct", "mb_edge"}
    assert "live" not in TUNABLE and "magic" not in TUNABLE
    assert all(not math.isnan(v) for v in ok.values() if isinstance(v, float))


def test_runtime_tune_applies_now_persists_and_resets(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    r = tr.cmd_tune(values={"small_account_batch_risk_pct": 20, "mb_threshold": 0.6, "live": 1})
    assert tr.cfg.small_account_batch_risk_pct == 20 and tr.cfg.mb_threshold == 0.6
    assert r["active"] == {"small_account_batch_risk_pct": 20.0, "mb_threshold": 0.6} and r["rejected"]
    assert any(l["msg"].startswith("TUNE") for l in tr.logs)
    assert json.load(open(tmp_path / "tune.json"))["mb_threshold"] == 0.6
    tr2, _ = make(tmp_path, {"BTCUSDm": series_up()})                # engine restart keeps the overrides
    assert tr2.cfg.mb_threshold == 0.6 and tr2._risk_view()["runtime_tuning"]
    tr2.cmd_tune(reset=True)
    assert tr2.cfg.mb_threshold == EngineConfig().mb_threshold and tr2.tune == {}


def _entries(tr):
    return [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")]


def test_small_account_opens_several_min_lots_inside_the_basket_cap(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, conservative(max_total_risk_pct=10.0))
    _small_account(br, balance=20.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 1, T0 + 40)
    assert len(_entries(tr)) >= 2, [l["msg"] for l in list(tr.logs)[-15:]]
    assert 0 < tr.risk.floor_risk <= 0.10 * 20.0 + 1e-6            # the largest basket stayed inside 10%


def test_small_account_refuses_a_min_lot_that_breaks_the_basket_cap(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, conservative(max_total_risk_pct=10.0))
    _small_account(br, balance=5.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    run(tr, br, T0 + 1, T0 + 20)
    assert not _entries(tr)
    assert any("basket risk budget" in json.dumps(d) for d in tr.decisions)


def test_backend_accepts_only_numeric_tune_values():
    import pytest
    from aiohttp import web

    from backend.server import _validate_cmd
    assert _validate_cmd("tune", {"values": {"mb_threshold": 0.6}}) == {"values": {"mb_threshold": 0.6}, "reset": False}
    assert _validate_cmd("tune", {"reset": True})["reset"] is True
    for bad in ({"values": {"mb_threshold": "0.6"}}, {"values": {"a b": 1}}, {"values": [1]},
                {"values": {f"k{i}": 1 for i in range(41)}}):
        with pytest.raises(web.HTTPBadRequest):
            _validate_cmd("tune", bad)


def test_launcher_saves_tune_for_the_next_start_when_the_engine_is_down(tmp_path):
    import asyncio

    from rapidburst.launcher import Launcher
    L = Launcher("http://127.0.0.1:9", "x" * 32, str(tmp_path))
    r = asyncio.run(L.handle("tune", {"values": {"mb_threshold": 0.62, "magic": 5}}))
    assert r["active"] == {"mb_threshold": 0.62} and r["rejected"]
    assert json.load(open(tmp_path / "tune.json")) == {"mb_threshold": 0.62}
    tr, _ = make(tmp_path, {"BTCUSDm": series_up()})
    assert tr.cfg.mb_threshold == 0.62
