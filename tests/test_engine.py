"""Unit tests for the tick momentum engine (SYNTHETIC ticks)."""
from rapidburst.config import EngineConfig
from rapidburst.engine import MomentumEngine, PositionState
from tests.synth import burst_series, gen_ticks

T0 = 1_760_000_000.0


def feed(eng, ticks):
    for t, b, a in ticks:
        eng.on_tick(t, b, a)


def first_signal(eng, ticks, after=0.0):
    """Feed tick by tick, return first non-HOLD signal at/after `after` (like the live loop)."""
    for t, b, a in ticks:
        eng.on_tick(t, b, a)
        s = eng.signal(t)
        if s.action != "HOLD" and t >= after:
            return s, t
    return None, None


def test_warmup_holds():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    feed(eng, burst_series(T0, 1, pre_seconds=10, burst_seconds=0))
    s = eng.signal()
    assert s.action == "HOLD" and "warming" in s.reason


def test_up_burst_gives_buy_quickly():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    ticks = burst_series(T0, +1)
    s, t = first_signal(eng, ticks, after=T0 + 150)
    assert s is not None and s.action == "BUY", s
    burst_start = T0 + 150
    assert t - burst_start < 3.0, f"entry too late: {t - burst_start:.2f}s after burst start"


def test_down_burst_gives_sell():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    s, t = first_signal(eng, burst_series(T0, -1, seed=7), after=T0 + 150)
    assert s is not None and s.action == "SELL" and t - (T0 + 150) < 3.0


def test_quiet_market_mostly_holds():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    ticks = burst_series(T0, 1, pre_seconds=300, burst_seconds=0, seed=3)
    n = sig = 0
    for t, b, a in ticks:
        eng.on_tick(t, b, a)
        s = eng.signal(t)
        if s.features.ready:
            n += 1
            sig += s.action != "HOLD"
    assert n > 100 and sig / n < 0.03, (sig, n)


def test_wide_spread_blocks_entry():
    cfg = EngineConfig(max_spread_points=10)
    eng = MomentumEngine("XAUUSD", 0.01, cfg)
    s, _ = first_signal(eng, burst_series(T0, 1))   # spread 18 pts > 10
    assert s is None
    assert "spread" in eng.signal().reason
    # same data with a normal cap does produce the burst entry
    s2, _ = first_signal(MomentumEngine("XAUUSD", 0.01, EngineConfig()), burst_series(T0, 1), after=T0 + 150)
    assert s2 is not None and s2.action == "BUY"


def test_idle_market_holds():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    ticks = burst_series(T0, 1)
    feed(eng, ticks)
    s = eng.signal(ticks[-1][0] + 60)
    assert s.action == "HOLD" and "idle" in s.reason


def test_extended_move_not_chased():
    """After a long one-way run that stalls, the engine must not buy the top."""
    cfg = EngineConfig()
    eng = MomentumEngine("XAUUSD", 0.01, cfg)
    ticks = burst_series(T0, 1, burst_seconds=40, step=0.05)
    t = ticks[-1][0]
    mid = (ticks[-1][1] + ticks[-1][2]) / 2
    # stall with a small uptick at the top
    for i in range(12):
        t += 0.3
        m = mid - 0.02 * i
        ticks.append((t, round(m - 0.09, 2), round(m + 0.09, 2)))
    feed(eng, ticks)
    s = eng.signal(t)
    assert s.action != "BUY", s.reason


def _pos(side, price, t, vu):
    return PositionState(ticket=1, symbol="XAUUSD", side=side, volume=0.1, open_price=price, open_time=t, entry_vu=vu)


def test_exit_profit_lock_and_reversal():
    cfg = EngineConfig()
    eng = MomentumEngine("XAUUSD", 0.01, cfg)
    ticks = [x for x in burst_series(T0, 1) if x[0] <= T0 + 152.5]   # mid-burst
    feed(eng, ticks)
    f = eng.features()
    vu = f.vol_unit
    # opened well below current price, peak recorded high, now gave back
    pos = _pos("BUY", f.bid - 1.0 * vu, f.t - 5, vu)
    pos.peak_units = 3.0
    r = eng.exit_decision(pos, f, f.t)
    assert r and ("profit lock" in r or "momentum" in r), r
    # strong reversal against a SELL
    pos2 = _pos("SELL", f.ask, f.t - 2, vu)
    r2 = eng.exit_decision(pos2, f, f.t)
    assert r2 and "reversal" in r2, r2


def test_exit_time_stop():
    cfg = EngineConfig(max_hold_seconds=60, reversal_score=2.0, weak_momentum_close_units=99)
    eng = MomentumEngine("XAUUSD", 0.01, cfg)
    ticks = burst_series(T0, 1, pre_seconds=200, burst_seconds=0)
    feed(eng, ticks)
    f = eng.features()
    pos = _pos("BUY", f.bid, f.t - 61, f.vol_unit)
    r = eng.exit_decision(pos, f, f.t)
    assert r and "time stop" in r


def test_signals_on_random_session_are_bounded():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    buys = sells = ready = 0
    for seed in (11, 12, 13):
        eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
        for t, b, a in gen_ticks(seed, T0, 1800):
            eng.on_tick(t, b, a)
            s = eng.signal(t)
            ready += s.features.ready
            buys += s.action == "BUY"
            sells += s.action == "SELL"
    assert buys > 0 and sells > 0
    # momentum signals are the exception, not the rule (no constant firing)
    assert (buys + sells) / ready < 0.15, (buys, sells, ready)
