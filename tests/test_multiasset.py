"""Multi-asset behaviour: sizing, thresholds and symbol handling for BTC, XAU and EURUSD.

SYNTHETIC specs/ticks (test doubles of what TickerAll/the broker return) - these tests prove the
engine/risk maths is symbol-agnostic, not that any market is quoting right now."""
import math
from tests.harness import legacy
import random
import time
from datetime import datetime, timezone

import pytest

from rapidburst.broker_tickerall import TickerAllBroker
from rapidburst.config import EngineConfig
from rapidburst.engine import MomentumEngine
from rapidburst.risk import AccountView, OpenPos, RiskGuard, size_position
from rapidburst.trader import Trader
from tests.fake_tickerall import API_KEY, DEMO, FakeTickerAll
from tests.test_tickerall import MemStore, mk, wait

EQUITY = 10_000.0
# (name, price, point, tick_size, tick_value_account, vol_min, vol_max, vol_step, typical spread (price units), sigma/tick)
ASSETS = {
    "BTCUSDm": dict(price=98_000.0, point=0.01, ts=0.01, tv=0.01, vmin=0.01, vmax=20, vstep=0.01, spread=18.0, sigma=6.0),
    "XAUUSD": dict(price=2_650.0, point=0.01, ts=0.01, tv=1.0, vmin=0.01, vmax=100, vstep=0.01, spread=0.18, sigma=0.035),
    "EURUSD": dict(price=1.1, point=1e-5, ts=1e-5, tv=1.0, vmin=0.01, vmax=100, vstep=0.01, spread=0.00012, sigma=0.00005),
}


def ticks(a, seconds=150, seed=3):
    rnd = random.Random(seed)
    t, mid, out = 1_000_000.0, a["price"], []
    while t < 1_000_000.0 + seconds:
        t += rnd.uniform(0.15, 0.5)
        mid += rnd.gauss(0, a["sigma"])
        out.append((t, mid - a["spread"] / 2, mid + a["spread"] / 2))
    return out


@pytest.mark.parametrize("name", list(ASSETS))
def test_engine_features_and_spread_gate_scale_with_the_symbol(name):
    a, cfg = ASSETS[name], legacy()
    eng = MomentumEngine(name, a["point"], cfg)
    for t, b, k in ticks(a):
        eng.on_tick(t, b, k)
    f = eng.features()
    assert f.ready, f.reason
    # volatility unit is in the symbol's own price units: a small, price-proportional number
    assert 0 < f.vol_unit / a["price"] < 0.01
    ok, why = eng.spread_ok(f)
    assert ok, (name, why)                                   # normal spread must not be blocked by a gold-sized cap
    wide = type(f)(**{**f.__dict__, "spread": a["price"] * 8e-4, "bid": f.mid, "ask": f.mid})   # 8 bp
    ok, why = eng.spread_ok(wide)
    assert not ok and "bp" in why                            # an 8bp spread is refused on every asset


@pytest.mark.parametrize("name", list(ASSETS))
def test_sizing_respects_budget_step_and_limits(name):
    a, cfg = ASSETS[name], legacy()
    sl_dist = a["price"] * 0.0012                            # ~12 bp stop
    res = size_position(EQUITY, cfg, sl_dist, a["ts"], a["tv"], a["vmin"], a["vmax"], a["vstep"])
    budget = EQUITY * cfg.risk_per_trade_pct / 100
    assert res.volume > 0, (name, res.note)
    assert res.volume <= cfg.max_lot_per_order and res.volume <= a["vmax"]
    assert abs(res.volume / a["vstep"] - round(res.volume / a["vstep"])) < 1e-6          # on the volume step
    assert res.risk_money <= budget * 1.5 + 1e-9
    # the loss at the stop really is risk_money (tick value maths, not gold-specific)
    assert res.risk_money == pytest.approx(sl_dist / a["ts"] * a["tv"] * res.volume)


def test_sizing_refuses_when_tick_value_unknown_or_min_lot_too_risky():
    cfg = legacy(max_lot_per_order=1.0)
    r = size_position(EQUITY, cfg, 100.0, 0.01, 0.0, 0.01, 20, 0.01)
    assert r.volume == 0 and "invalid" in r.note
    r = size_position(EQUITY, cfg, 5_000.0, 0.01, 0.01, 2.0, 20, 2.0)   # broker min lot above max_lot_per_order
    assert r.volume == 0 and "max_lot_per_order" in r.note


def test_total_exposure_cap_is_notional_based_across_assets():
    cfg = legacy(max_total_notional_x=2.0, max_total_lots=1000, max_total_risk_pct=100)
    g = RiskGuard(cfg)
    acct = AccountView(EQUITY, EQUITY, EQUITY, 0.0)
    held = [OpenPos("BTCUSDm", "BUY", 0.15, 0.0, 10.0, notional=0.15 * 98_000)]            # 14.7k = 1.47x equity
    ok, _ = g.check_entry("EURUSD", "BUY", held, acct, 0.1, 10.0, 10.0, True, new_notional=0.1 * 100_000 * 1.1 * 0.4)
    assert ok                                                                               # 4.4k more: 1.9x
    ok, why = g.check_entry("EURUSD", "BUY", held, acct, 0.1, 10.0, 10.0, True, new_notional=11_000)
    assert not ok and "exposure" in why
    cfg2 = legacy(max_positions_per_symbol=1)
    ok, why = RiskGuard(cfg2).check_entry("BTCUSDm", "BUY", held, acct, 0.01, 1.0, 1.0, True)
    assert not ok and "positions on BTCUSDm" in why                                         # per-symbol limit kept


# ------------------------------------------------------------------ TickerAll adapter + trader (mocked API)
@pytest.fixture
def fake():
    f = FakeTickerAll()
    f.start()
    yield f
    f.stop()


def connected(fake, tmp_path):
    b = mk(fake, tmp_path)
    assert b.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    return b


def test_catalogue_search_by_name_class_alias_and_paging(fake, tmp_path):
    b = connected(fake, tmp_path)
    names = lambda q: [x["name"] for x in b.search_symbols(q)]
    assert set(names("")) == {"XAUUSD", "EURUSD", "BTCUSDm", "BTCUSD", "ETHUSDm", "US30m"}     # the full list
    assert {"BTCUSDm", "BTCUSD"} <= set(names("bitcoin")) and "XAUUSD" not in names("btc")
    assert set(names("crypto")) >= {"BTCUSDm", "ETHUSDm"}
    assert names("gold") == ["XAUUSD"]
    assert "EURUSD" in names("fx") and "eur usd" not in names("zzz")
    d = {x["name"]: x["desc"] for x in b.search_symbols("")}
    assert "derived" in d["BTCUSDm"] and "BTC/USD" in d["BTCUSDm"]
    b.shutdown()


def test_tick_value_derived_only_when_exact(fake, tmp_path):
    b = connected(fake, tmp_path)
    btc = b.symbol_info("BTCUSDm")
    assert btc["trade_tick_value_loss"] == pytest.approx(0.01) and "derived" in btc["tick_value_source"]
    eth = b.symbol_info("ETHUSDm")                         # quoted in EUR on a USD account: cannot be derived
    assert eth["trade_tick_value_loss"] == 0 and "unknown" in eth["tick_value_source"]
    b.shutdown()


def test_no_default_symbol_and_start_needs_a_tradable_quoting_symbol(fake, tmp_path):
    fake.auto_ticks = True
    tr = Trader(connected(fake, tmp_path), MemStore(dict(DEMO)), legacy(), str(tmp_path))
    assert tr.selected == []                               # no hardcoded XAUUSD
    tr.step(time.time())
    chk = {c["name"]: c for c in tr.validation["checks"]}["Tradable symbol selected"]
    assert not chk["ok"] and "no symbol selected" in chk["detail"]
    r = tr.cmd_start()
    assert not r["ok"] and "Tradable symbol" in r["msg"]
    r = tr.cmd_select(["ETHUSDm"])                         # tick value cannot be computed -> clear reason
    assert r["ok"] and not r["validation"]["ok"]
    rows = {c["name"]: c for c in r["validation"]["checks"]}
    assert "tick value unknown" in rows["Symbol ETHUSDm"]["detail"] and rows["Symbol ETHUSDm"]["soft"]
    r = tr.cmd_select(["BTCUSDm", "XAUUSD", "EURUSD"])     # three assets at once
    assert r["ok"] and r["validation"]["ok"], r["validation"]
    tr.cmd_shutdown()


def test_closed_symbol_is_skipped_not_blocking_and_resumes(fake, tmp_path):
    """Only BTC quotes (weekend); XAU/EURUSD are 'closed'. START works on BTC, the others show
    'no quotes' and pick up automatically once quotes arrive."""
    b = connected(fake, tmp_path)
    tr = Trader(b, MemStore(dict(DEMO)), legacy(), str(tmp_path))
    tr.step(time.time())
    assert tr.cmd_select(["BTCUSDm", "XAUUSD"])["ok"] is True      # selection itself never needs quotes
    assert wait(lambda: any(c.get("kind") == "ticks" for c in fake.subscriptions))
    fake.push_tick("BTCUSDm", 98000.0, 98018.0)
    fake.push_tick("XAUUSD", 2650.0, 2650.18, stale=True)          # last-known price, NOT live
    assert b.quote_state("BTCUSDm") == "live" and b.quote_state("XAUUSD") == "closed"
    r = tr.cmd_start()
    assert r["ok"], r
    rows = {c["name"]: c for c in tr.validation["checks"]}
    assert rows["Symbol BTCUSDm"]["ok"] and not rows["Symbol XAUUSD"]["ok"]
    assert "no live quotes" in rows["Symbol XAUUSD"]["detail"]
    tr.step(time.time()); tr._build_snapshot(time.time())
    view = tr.snapshot["engine"]["symbols"]
    assert view["BTCUSDm"]["market"] == "live" and view["XAUUSD"]["market"] == "closed"
    assert "resumes automatically" in view["XAUUSD"]["reason"]
    fake.push_tick("XAUUSD", 2651.0, 2651.18)                      # market opens
    assert wait(lambda: b.quote_state("XAUUSD") == "live")
    tr.step(time.time()); tr._build_snapshot(time.time() + 2)
    assert tr.snapshot["engine"]["symbols"]["XAUUSD"]["market"] == "live"
    assert tr.cmd_stop()["ok"]
    tr.cmd_shutdown()


def test_start_blocked_with_reason_when_every_selected_symbol_is_closed(fake, tmp_path):
    b = connected(fake, tmp_path)
    tr = Trader(b, MemStore(dict(DEMO)), legacy(), str(tmp_path))
    tr.step(time.time())
    tr.cmd_select(["XAUUSD"])
    r = tr.cmd_start()
    assert not r["ok"] and "quoting now" in r["msg"]
    chk = {c["name"]: c for c in tr.validation["checks"]}["A selected symbol is quoting now"]
    assert "closed" in chk["detail"] and "crypto" in chk["detail"]
    tr.cmd_shutdown()


def test_search_paging_through_trader(fake, tmp_path):
    tr = Trader(connected(fake, tmp_path), MemStore(dict(DEMO)), legacy(), str(tmp_path))
    tr.step(time.time())
    p1 = tr.cmd_search("", 0, 4)
    p2 = tr.cmd_search("", 4, 4)
    assert p1["total"] == 6 and p1["has_more"] and len(p1["symbols"]) == 4 and not p2["has_more"]
    assert {x["name"] for x in p1["symbols"] + p2["symbols"]} == {"XAUUSD", "EURUSD", "BTCUSDm", "BTCUSD", "ETHUSDm", "US30m"}
    tr.cmd_shutdown()
