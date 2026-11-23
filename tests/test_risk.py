"""Unit tests for sizing, exposure limits, halts and pacing."""
from rapidburst.config import EngineConfig
from tests.harness import legacy, conservative
from rapidburst.risk import (AccountView, EntryPacer, OpenPos, RiskGuard, floor_to_step, normalize_volume,
                             size_position)

ACCT = AccountView(balance=10000, equity=10000, margin_free=9000, margin_level=0)


def test_volume_normalisation():
    assert floor_to_step(0.237, 0.01) == 0.23
    assert normalize_volume(0.237, 0.01, 100, 0.01) == 0.23
    assert normalize_volume(0.004, 0.01, 100, 0.01) == 0.0
    assert normalize_volume(500, 0.01, 50, 0.01) == 50
    assert normalize_volume(1.7, 0.1, 100, 0.1) == 1.7


def test_size_xauusd():
    cfg = legacy(risk_per_trade_pct=0.25)
    # SL 1.50 USD on XAUUSD: 150 ticks * $1 per tick per lot = $150/lot; budget $25 -> 0.16 lot
    r = size_position(10000, cfg, 1.50, 0.01, 1.0, 0.01, 100, 0.01)
    assert r.volume == 0.16 and abs(r.risk_money - 24.0) < 1e-6


def test_size_min_lot_too_risky_is_rejected():
    cfg = conservative(risk_per_trade_pct=0.25)
    r = size_position(200, cfg, 15.0, 0.01, 1.0, 0.01, 100, 0.01)  # min lot risk $15 = 7.5% of $200
    assert r.volume == 0.01 and "min lot floor" in r.note           # no minimum-balance rule
    r = size_position(200, conservative(small_account_max_loss_pct=5), 15.0, 0.01, 1.0, 0.01, 100, 0.01)
    assert r.volume == 0.0 and "small-account cap" in r.note        # optional cap still available


def test_size_small_account_min_lot_fallback():
    # $10 Exness demo, BTCUSDm: tick 0.01, $0.01/tick/lot, SL $25 -> min lot 0.01 loses $0.25 (2.5%)
    cfg = conservative()
    r = size_position(10.0, cfg, 25.0, 0.01, 0.01, 0.01, 200, 0.01)
    assert r.volume == 0.01 and abs(r.risk_money - 0.25) < 1e-9 and "min lot floor" in r.note
    # SL $60 -> $0.60 = 6% of $10: still trades (no minimum balance); $2 account too
    assert size_position(10.0, cfg, 60.0, 0.01, 0.01, 0.01, 200, 0.01).volume == 0.01
    assert size_position(2.0, cfg, 25.0, 0.01, 0.01, 0.01, 200, 0.01).volume == 0.01
    # optional configurable cap
    capped = conservative(small_account_max_loss_pct=5)
    r = size_position(10.0, capped, 60.0, 0.01, 0.01, 0.01, 200, 0.01)
    assert r.volume == 0.0 and "0.60" in r.note
    assert size_position(10.0, conservative(small_account_max_loss_pct=10), 60.0, 0.01, 0.01, 0.01, 200, 0.01).volume == 0.01
    # lots scale up automatically as equity grows
    assert size_position(10_000.0, cfg, 25.0, 0.01, 0.01, 0.01, 200, 0.01).volume > 0.01


def test_floor_mode_total_risk_cap_and_free_margin():
    from rapidburst.risk import AccountView, RiskGuard
    g = RiskGuard(legacy(max_total_risk_pct=10))
    ok, why = g.check_entry("BTCUSDm", "BUY", [], AccountView(3.0, 3.0, 2.0, 0.0), 0.01, 0.4, 0.45, True, 900.0,
                            floor_mode=True)
    assert not ok and "total stop-risk cap" in why          # 13% of equity > 10% total cap
    g = RiskGuard(legacy())                           # t30u default: 100% of equity across all baskets
    ok, why = g.check_entry("BTCUSDm", "BUY", [], AccountView(3.0, 3.0, 2.0, 0.0), 0.01, 0.4, 0.45, True, 900.0,
                            floor_mode=True)
    assert ok, why
    ok, why = g.check_entry("BTCUSDm", "BUY", [], AccountView(3.0, 3.0, 0.3, 0.0), 0.01, 0.4, 0.45, True, 900.0,
                            floor_mode=True)
    assert not ok and "insufficient free margin for the minimum lot" in why

def test_size_capped_by_max_lot():
    cfg = legacy(risk_per_trade_pct=5, max_lot_per_order=0.5)
    r = size_position(1_000_000, cfg, 1.0, 0.01, 1.0, 0.01, 100, 0.01)
    assert r.volume == 0.5


def test_limits():
    cfg = conservative(max_positions_total=3, max_positions_per_symbol=2, max_total_lots=1.0, max_total_risk_pct=1.0)
    g = RiskGuard(cfg)
    win = OpenPos("XAUUSD", "BUY", 0.2, 5.0, 10)
    lose = OpenPos("XAUUSD", "BUY", 0.2, -5.0, 10)
    assert g.check_entry("XAUUSD", "BUY", [], ACCT, 0.2, 10, 100, False)[0]
    ok, why = g.check_entry("XAUUSD", "BUY", [lose], ACCT, 0.2, 10, 100, True)
    assert not ok and "averaging" in why
    ok, why = g.check_entry("XAUUSD", "SELL", [win], ACCT, 0.2, 10, 100, True)
    assert not ok and "opposite" in why
    assert not g.check_entry("XAUUSD", "BUY", [win], ACCT, 0.2, 10, 100, False)[0]  # add needs strong score
    assert g.check_entry("XAUUSD", "BUY", [win], ACCT, 0.2, 10, 100, True)[0]
    ok, why = g.check_entry("XAUUSD", "BUY", [win, win], ACCT, 0.2, 10, 100, True)
    assert not ok and "max 2" in why
    ok, why = g.check_entry("EURUSD", "BUY", [win, win, win], ACCT, 0.2, 10, 100, True)
    assert not ok and "max positions" in why
    ok, why = g.check_entry("EURUSD", "BUY", [win], ACCT, 0.9, 10, 100, True)
    assert not ok and "lots" in why
    ok, why = g.check_entry("EURUSD", "BUY", [win], ACCT, 0.1, 95, 100, True)
    assert not ok and "risk" in why
    ok, why = g.check_entry("EURUSD", "BUY", [], ACCT, 0.1, 10, 5000, True)
    assert not ok and "margin" in why


def test_safe_mode_instead_of_daily_halt():
    cfg = legacy(equity_drawdown_pct=5, safe_pause_s=300, safe_pause_max_s=1800, safe_probe_batches=3)
    g = RiskGuard(cfg)
    g.reset_session(10000)
    assert g.update(0, ACCT) is None
    assert g.update(1, AccountView(10000, 9600, 9000, 0)) is None          # -4%: fine (no daily-loss stop)
    ev = g.update(2, AccountView(10000, 9490, 9000, 0))
    assert ev[0] == "safe" and g.halted and "SAFE MODE" in ev[1]
    assert not g.check_entry("XAUUSD", "BUY", [], ACCT, 0.1, 1, 1, False)[0]
    assert g.update(200, AccountView(10000, 9490, 9000, 0)) is None and g.halted
    ev = g.update(303, AccountView(10000, 9490, 9000, 0))
    assert ev[0] == "resume" and not g.halted and g.probe_left == 3 and g.peak_equity == 9490
    g.update(304, AccountView(10000, 9000, 9000, 0))                        # repeat => longer pause
    assert g.halted and g.safe_until - 304 == 600


def test_pacer_has_no_fixed_cooldowns_and_streak_only_raises_the_bar():
    cfg = legacy(max_entries_per_minute=3, loss_streak_threshold_add=0.03)
    p = EntryPacer(cfg)
    assert p.can_enter("X", 0)[0]
    p.on_entry("X", 0)
    assert p.can_enter("X", 0.1)[0]                       # immediate re-entry allowed (no cooldown)
    p.on_entry("X", 1); p.on_entry("X", 2)
    ok, why = p.can_enter("X", 3)
    assert not ok and "minute" in why                    # API guard only
    assert p.can_enter("X", 61)[0]
    for i in range(5):
        p.on_close("X", -10, 100 + i)
        assert p.can_enter("X", 100 + i)[0]               # losses never pause...
    assert abs(p.threshold_add("X") - 0.09) < 1e-9       # ... they raise the signal bar (max 3 steps)
    p.on_close("X", +5, 200)
    assert p.threshold_add("X") == 0.0
    p.inflight.add("X")
    assert "in flight" in p.can_enter("X", 9999)[1]


def test_min_lot_floor_total_cap_and_catastrophic_safe_mode():
    cfg = legacy(max_total_risk_pct=100.0, small_account_batch_risk_pct=50.0, equity_drawdown_pct=50.0,
                       safe_dd_cap_pct=50.0)            # the t30u aggressive limits (t32u defaults are lower)
    g = RiskGuard(cfg)
    g.reset_session(13.0)
    small = AccountView(13, 13, 12, 0)
    one = OpenPos("BTCUSDm", "BUY", 0.01, -0.1, 4.0)
    assert g.check_entry("BTCUSDm", "BUY", [one, one], small, 0.01, 4.0, 1.0, True, batch_leg=True, floor_mode=True)[0]
    ok, why = g.check_entry("BTCUSDm", "BUY", [one, one, one], small, 0.01, 2.0, 1.0, True, batch_leg=True, floor_mode=True)
    assert not ok and "total stop-risk cap" in why       # 14 > 100% of 13
    g.floor_risk = 4.0
    assert abs(g.dd_limit() - 6.5) < 1e-9                # safe mode only at a 50% drawdown
    assert g.update(1, AccountView(13, 7.0, 6, 0)) is None
    assert g.update(2, AccountView(13, 6.4, 6, 0))[0] == "safe"
