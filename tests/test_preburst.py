"""Ultra-Fast Pre-Burst engine: incremental scanners vs brute force, O(1)-per-tick cost, A/B/C entry rules,
burst batch (caps, cancel-if-run, no adds), latency fields, double-close guard. SimBroker + SYNTHETIC ticks
only (test double, not market data)."""
import random
import time
from collections import deque

import rapidburst.microburst as mb
from rapidburst.broker_tickerall import TickerAllBroker
from rapidburst.config import EngineConfig
from rapidburst.microburst import Micro, TickScanner, _Ext, _Roll, decide
from tests.harness import make, run
from tests.paths import T0, _force_signal, _small_account, _start, series_up


def _stream(n, seed=3, t0=1000.0):
    rnd = random.Random(seed)
    t, p, out = t0, 100.0, []
    for _ in range(n):
        t += rnd.expovariate(4.0)
        p += rnd.gauss(0, 0.02)
        out.append((t, p))
    return out


def test_incremental_windows_match_brute_force():
    roll, ext = _Roll(3.0), _Ext(10.0, lag=2.0)
    ticks = _stream(3000)
    prev = None
    for i, (t, p) in enumerate(ticks):
        d = 0.0 if prev is None else p - prev
        prev = p
        roll.push(t, p, d, 0, 0)
        roll.expire(t)
        ext.push(t, p)
        ext.update(t)
        if i % 97 == 0 and i > 50:
            win = [(tt, pp) for tt, pp in ticks[:i + 1] if tt >= t - 3.0]
            ds = [pp - ticks[k - 1][1] for k, (tt, pp) in enumerate(ticks[:i + 1]) if k and tt >= t - 3.0]
            assert roll.n == len(win)
            assert abs(roll.sd - sum(ds)) < 1e-9 and abs(roll.sa - sum(abs(x) for x in ds)) < 1e-9
            lagged = [pp for tt, pp in ticks[:i + 1] if t - 10.0 <= tt <= t - 2.0]
            if lagged:
                assert ext.ok and ext.hi == max(lagged) and ext.lo == min(lagged)


def test_scanner_cost_per_tick_does_not_grow_with_history():
    cfg = EngineConfig()

    def per_tick(n):
        sc = TickScanner(cfg)
        ticks = _stream(n, seed=5)
        t0 = time.perf_counter()
        for t, p in ticks:
            sc.push(t, p - 0.05, p + 0.05)
            sc.snapshot(t, 1.0, 0.1, [], cfg, [])
        return (time.perf_counter() - t0) / n, sc
    small, _ = per_tick(1500)
    big, sc = per_tick(15000)
    assert big < small * 3.0, (small, big)                 # no per-tick rescans of a growing history
    assert len(sc.r60.q) < 1000 and len(sc.r_vel.q) < 100  # windows stay bounded by time


def test_ticker_all_ticks_since_returns_only_new_ticks_without_copying():
    br = TickerAllBroker.__new__(TickerAllBroker)
    br._ticks = {"X": deque([(i / 1000, 1.0, 1.1, i) for i in range(1, 5001)], maxlen=5000)}
    assert br.ticks_since("X", 5000) == []
    out = br.ticks_since("X", 4997)
    assert [t[3] for t in out] == [4998, 4999, 5000]
    assert len(br.ticks_since("X", 0, max_count=10)) == 10


def _decide_with(a, b, c, d=None, e=None):
    cfg = EngineConfig()
    real = mb.side_strength

    def fake(m, side, cfg_):
        return (0.9, {}, (a, b, c, d, e), []) if side > 0 else (0.0, {}, (0.0, 0.0, 0.0, d and 0.0, e and 0.0), [])
    mb.side_strength = fake
    try:
        return decide(Micro(ready=True, stable=True, regime="fast", spread=0.1, atr=1.0, vu=0.1, target=0.3), cfg)
    finally:
        mb.side_strength = real


def test_agreement_rule_no_single_indicator_speed_only_or_volume_only():
    for fam in ((0.9, 0.1, 0.1), (0.1, 0.9, 0.1), (0.9, 0.6, 0.1, 0.1, 0.1), (0.1, 0.9, 0.1, 0.1, 0.9)):
        st = _decide_with(*fam)
        assert st.side == "NO TRADE" and "families agree" in st.reason, (fam, st.reason)
    assert _decide_with(0.9, 0.6, 0.1).side == "BUY"                 # no bars yet: 2 of A/B/C
    assert _decide_with(0.9, 0.1, 0.6).side == "BUY"
    st = _decide_with(0.9, 0.6, 0.1, 0.5, 0.1)                       # 3 of 5 incl. M1 timing
    assert st.side == "BUY" and "agree ABD" in st.reason
    assert _decide_with(0.2, 0.2, 0.2, 0.6, 0.6).side == "NO TRADE"  # 2 of 5
    assert _decide_with(0.6, 0.2, 0.2, 0.6, 0.6).side == "BUY"       # tick + M1 + M5 agree, no volume needed


def _forced_fast(tr):
    _force_signal(tr, "BUY", origin=T0 + 99.0)
    real = tr._micro

    def fast(s, now_wall):
        m, st = real(s, now_wall)
        m.regime = "fast"
        return m, st
    tr._micro = fast


def test_batch_size_caps_latency_and_cancel_if_price_ran(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    for t in list(br.positions_):
        br.close_position(t, 50, tr.cfg.magic)
    run(tr, br, T0 + 100.0, T0 + 100.1)
    tr.pacer = type(tr.pacer)(tr.cfg)
    k0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    _forced_fast(tr)
    run(tr, br, T0 + 100.3, T0 + 100.6)
    entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")][k0:]
    assert 1 <= len(entries) <= tr.cfg.max_orders_per_batch
    assert all("latency tick->decision" in e and "send->fill verified" in e for e in entries)
    lat = tr.lat_entries[-1]
    assert {"tick_to_decision_ms", "decision_to_send_ms", "send_to_fill_verified_ms", "tick_to_fill_ms"} <= set(lat)
    # second batch: price runs far past the edge after the first order -> the rest is cancelled
    for t in list(br.positions_):
        br.close_position(t, 50, tr.cfg.magic)
    run(tr, br, T0 + 100.9, T0 + 101.5)
    tr.pacer = type(tr.pacer)(tr.cfg)
    tr.last_burst.clear()
    real_lt, real_mo = br.last_tick, br.market_order
    sent = []

    def mo(*a, **k):
        sent.append(1)
        return real_mo(*a, **k)

    def ran(s):                                    # price runs away right after the first order of the batch
        q = dict(real_lt(s))
        if sent:
            q["bid"], q["ask"] = q["bid"] + 1000.0, q["ask"] + 1000.0
        return q
    br.last_tick, br.market_order = ran, mo
    n0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 101.8, T0 + 102.1)
    br.last_tick, br.market_order = real_lt, real_mo
    new = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")][n0:]
    batches = [l["msg"] for l in tr.logs if l["msg"].startswith("BATCH ")]
    assert len(new) == 1 and "past the edge" in batches[-1], (new, batches[-1:])
    assert "re-entry" in batches[-1]               # measured close -> next entry


def test_anti_chase_before_send_cancels_the_whole_batch(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    for t in list(br.positions_):
        br.close_position(t, 50, tr.cfg.magic)
    run(tr, br, T0 + 100.0, T0 + 100.1)
    _forced_fast(tr)
    real_lt = br.last_tick
    br.last_tick = lambda s: dict(real_lt(s), bid=real_lt(s)["bid"] + 1000.0, ask=real_lt(s)["ask"] + 1000.0)
    n0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 100.3, T0 + 100.6)
    assert len([l for l in tr.logs if l["msg"].startswith("ENTRY")]) == n0
    assert any("before send" in d["outcome"] for d in tr.decisions)


def test_parallel_batch_send_on_a_thread_safe_broker(tmp_path):
    import threading
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    for t in list(br.positions_):
        br.close_position(t, 50, tr.cfg.magic)
    run(tr, br, T0 + 100.0, T0 + 100.1)
    _forced_fast(tr)
    lock, real_mo, threads = threading.Lock(), br.market_order, set()

    def mo(*a, **k):
        threads.add(threading.current_thread().name)
        with lock:
            return real_mo(*a, **k)
    br.market_order = mo
    br.concurrent_io = True
    k0 = len([l for l in tr.logs if l["msg"].startswith("ENTRY")])
    run(tr, br, T0 + 100.3, T0 + 100.6)
    new = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")][k0:]
    assert len(new) >= 2 and all(t.startswith("rb-io") for t in threads), (new, threads)


def test_open_basket_is_never_added_to(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    _forced_fast(tr)
    run(tr, br, T0 + 100.3, T0 + 100.6)
    n = len(br.positions_)
    assert n >= 1
    tr._micro_exit = lambda *a, **k: None          # keep the basket open
    tr._hard_risk_exit = lambda *a, **k: None
    run(tr, br, T0 + 100.9, T0 + 110)
    assert len(br.positions_) == n
    assert "no adds" in tr.sym_status["BTCUSDm"]["reason"]


def test_verified_closed_ticket_is_never_closed_twice(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 100)
    _forced_fast(tr)
    run(tr, br, T0 + 100.3, T0 + 100.6)
    assert br.positions_
    calls = []
    real_close = br.close_position

    def close(t, dev, magic):
        calls.append(t)
        return real_close(t, dev, magic)
    br.close_position = close
    stale = {t: dict(p) for t, p in br.positions_.items()}
    tr._hard_risk_exit = lambda *a, **k: "hard group loss (test)"
    run(tr, br, T0 + 100.9, T0 + 101.0)
    assert calls and not br.positions_
    # a stale cached broker list still showing the closed tickets must not trigger a second close
    real_pos = br.positions
    br.positions = lambda *a, **k: list(stale.values()) + real_pos()
    n = len(calls)
    run(tr, br, T0 + 101.3, T0 + 102.5)
    br.positions = real_pos
    assert len(calls) == n and len(set(calls)) == len(calls)
    assert tr.lat_exits and "close_ms" in tr.lat_exits[-1]


def test_env_report_flags_stale_vars_and_never_shows_secret_values(monkeypatch):
    monkeypatch.setenv("RB_DAILY_LOSS_PCT", "3")
    monkeypatch.setenv("RB_BASKET_ENABLED", "1")
    monkeypatch.setenv("RB_MAX_MARGIN_USE_PCT", "70")
    monkeypatch.setenv("RB_UI_PASSWORD", "very-secret-value")
    r = EngineConfig.env_report()
    assert r["applied"] == {"RB_MAX_MARGIN_USE_PCT": "70"}
    assert {"RB_DAILY_LOSS_PCT", "RB_BASKET_ENABLED"} <= set(r["ignored_env"])
    assert "very-secret-value" not in repr(r) and "RB_UI_PASSWORD" not in r["ignored_env"]
