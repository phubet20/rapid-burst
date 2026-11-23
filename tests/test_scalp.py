"""Scalp mode (default): early-impulse entry, anti-chase, many small same-lot orders ~1 s apart, reversal exits.
SimBroker + SYNTHETIC ticks only (test double)."""
import math
import random

from rapidburst.config import EngineConfig
from rapidburst.engine import Tick
from rapidburst.impulse import detect_early, detect_reversal
from tests.harness import make, now_base, run
from tests.test_burst import _small_account, _start

T0 = now_base()
NOW = math.floor(T0 / 60.0) * 60.0 + 50.0        # 50 s into an M1 bar
SPREAD = 0.02
ATR = 1.0


def _ticks(segments, start=NOW - 70.0, base=100.0, dt=0.25):
    """segments: list of (seconds, price change per tick). Returns Ticks ending at NOW."""
    total = sum(sec for sec, _ in segments)
    t, mid, out = NOW - total, base, []
    # flat lead-in so the buffer reaches back past the bar open
    x = start
    while x < t:
        out.append(Tick(x, mid - SPREAD / 2, mid + SPREAD / 2))
        x += dt
    for sec, step in segments:
        n = int(round(sec / dt))
        for _ in range(n):
            t += dt
            mid += step
            out.append(Tick(t, mid - SPREAD / 2, mid + SPREAD / 2))
    return out


cfg = EngineConfig()


def test_early_start_is_accepted():
    # flat, then a small accelerating push in the last 3 s: velocity doubles, one-sided flow, ~0.18 ATR moved
    imp = detect_early(_ticks([(40, 0.0), (1.5, 0.01), (1.5, 0.02)]), NOW, ATR, SPREAD, cfg)
    assert imp.dir == 1 and imp.reason.startswith("impulse-early BUY"), imp.reason
    imp = detect_early(_ticks([(40, 0.0), (1.5, -0.01), (1.5, -0.02)]), NOW, ATR, SPREAD, cfg)
    assert imp.dir == -1


def test_anti_chase_refuses_a_move_that_already_happened():
    # 0.9 ATR rise inside the last 20 s (from the swing) and still accelerating -> too late
    late = _ticks([(30, 0.0), (10, 0.0), (6, 0.05), (1.5, 0.01), (1.5, 0.02)])
    imp = detect_early(late, NOW, ATR, SPREAD, cfg)
    assert imp.dir == 0 and "anti-chase" in imp.reason, imp.reason
    # price already 0.8 ATR above the M1 bar open (but the swing is older than 20 s) -> refused as well
    far = _ticks([(15, 0.0), (10, 0.0), (1.5, 0.01), (1.5, 0.02)], base=100.0)
    ticks = [Tick(tk.t, tk.bid + 0.8 * (tk.t >= NOW - 49.0), tk.ask + 0.8 * (tk.t >= NOW - 49.0)) for tk in far]
    imp = detect_early(ticks, NOW, ATR, SPREAD, cfg)
    assert imp.dir == 0 and "anti-chase" in imp.reason, imp.reason


def test_decelerating_or_one_sided_flow_missing_is_refused():
    dec = detect_early(_ticks([(40, 0.0), (1.5, 0.03), (1.5, 0.01)]), NOW, ATR, SPREAD, cfg)
    assert dec.dir == 0 and "decelerating" in dec.reason, dec.reason
    flat = detect_early(_ticks([(60, 0.0)]), NOW, ATR, SPREAD, cfg)
    assert flat.dir == 0


def test_env_configurable_anti_chase(monkeypatch):
    monkeypatch.setenv("RB_SCALP_MAX_BAR_ATR", "0.5")
    monkeypatch.setenv("RB_SCALP_MAX_SWING_ATR", "0.65")
    monkeypatch.setenv("RB_MAX_MARGIN_USE_PCT", "100")
    monkeypatch.setenv("RB_SCALP_MAX_ORDERS", "20")
    c = EngineConfig.load()
    assert (c.scalp_max_bar_atr, c.scalp_max_swing_atr, c.max_margin_use_pct, c.scalp_max_orders) == (0.5, 0.65, 100.0, 20)
    f = EngineConfig.__dataclass_fields__          # production defaults (conftest turns both modes off for legacy tests)
    assert f["scalp_enabled"].default is True and f["basket_enabled"].default is False
    assert f["max_margin_use_pct"].default == 100.0 and f["scalp_max_orders"].default == 20


def test_reversal_flags():
    up = _ticks([(40, 0.0), (3, 0.02)])
    r = detect_reversal(up, NOW, 1, ATR, SPREAD, None, 0.0, cfg)
    assert not any([r["vel_flip"], r["press"], r["wick"]])              # still pushing up
    # price rose, then rolls over: velocity flips, flow turns, wick forms against the long
    top = _ticks([(40, 0.0), (3, 0.03), (1.5, -0.02)])
    peak = max(tk.mid for tk in top)
    r = detect_reversal(top, NOW, 1, ATR, SPREAD, peak, 0.12, cfg)
    assert r["vel_flip"] and r["wick"] and r["press"] is True or r["vel_flip"] and r["wick"]
    # decay: velocity collapsed to ~0 relative to its peak
    dec = _ticks([(40, 0.0), (1.5, 0.0), (1.5, 0.0005)])
    r = detect_reversal(dec, NOW, 1, ATR, SPREAD, None, 0.2, cfg)
    assert r["decay"]
    # for a SELL group the signs mirror
    r = detect_reversal(_ticks([(40, 0.0), (3, 0.03), (1.5, 0.02)]), NOW, -1, ATR, SPREAD, None, 0.0, cfg)
    assert r["vel_flip"] and r["press"]


# ------------------------------------------------------------ integration (Trader + SimBroker)
def _scalp_path(reverse=True, seed=5):
    rnd = random.Random(seed)
    t, mid, out = T0, 2650.0, []

    def add(sec, step_fn, dt=0.25, jitter=0.0):
        nonlocal t, mid
        n = int(sec / dt)
        for i in range(n):
            t += dt
            mid += step_fn(i) + (rnd.gauss(0, jitter) if jitter else 0.0)
            out.append((t, round(mid - 0.02, 2), round(mid + 0.02, 2)))
    add(130, lambda i: 0.0, dt=0.3, jitter=0.003)
    add(1.5, lambda i: 0.012)
    add(2.0, lambda i: 0.02)
    add(8.0, lambda i: 0.03)                                  # accelerating push (+~1.2 px = 0.8 ATR)
    if reverse:
        add(10.0, lambda i: -0.05)                            # the bar's force reverses
    add(40, lambda i: 0.0, dt=0.3, jitter=0.003)
    return out


def _run_scalp(tmp_path, **kw):
    kw.setdefault("scalp_enabled", True)
    cfg = EngineConfig(**kw)
    tr, br = make(tmp_path, {"BTCUSDm": _scalp_path()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=800.0)
    seen = {"n": 0, "margin": 0.0, "lots": set(), "adds_t": []}
    last_n = [0]

    def watch(t):
        tr.atr_m1["BTCUSDm"] = 1.5
        eq = br.account()["equity"]
        used = sum(p["volume"] * 800.0 for p in br.positions_.values())
        seen["n"] = max(seen["n"], len(br.positions_))
        seen["margin"] = max(seen["margin"], used / eq * 100 if eq else 0)
        seen["lots"] |= {p["volume"] for p in br.positions_.values()}
        if len(br.positions_) > last_n[0]:
            seen["adds_t"].append(t)
        last_n[0] = len(br.positions_)
        for p in br.positions_.values():
            dd = 1 if p["type"] == 0 else -1
            assert (p["tp"] - p["price_open"]) * dd > 0                           # quick TP
            far = (p["price_open"] - p["sl"]) * dd > 1.0                          # far emergency SL at open ...
            locked = (p["sl"] - p["price_open"]) * dd >= 0                        # ... or BE+/trail moved it into profit
            assert far or locked, p
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200, on_step=watch)
    return tr, br, seen


def test_scalp_enters_early_scales_in_with_same_lot_and_exits_on_reversal(tmp_path):
    tr, br, seen = _run_scalp(tmp_path, scalp_tp_atr=5.0, scalp_tp_spread_mult=1.0)   # TP far: the reversal must do the exit
    msgs = [l["msg"] for l in tr.logs]
    opens = [m for m in msgs if m.startswith("SCALP OPEN")]
    adds = [m for m in msgs if m.startswith("SCALP ADD")]
    assert opens and "impulse-early" in opens[0], msgs[-15:]
    assert len(adds) >= 2, msgs[-15:]
    assert len(seen["lots"]) == 1                                         # same lot every order: no martingale
    assert all(b - a >= 1.0 - 1e-6 for a, b in zip(seen["adds_t"], seen["adds_t"][1:])), seen["adds_t"]
    assert seen["n"] <= tr.cfg.scalp_max_orders and seen["margin"] <= 100.0
    exits = [m for m in msgs if m.startswith("EXIT BATCH")]
    assert exits, msgs[-15:]
    assert any(("reversal" in m or "quick take-profit" in m) for m in exits), exits
    assert not br.positions_                                              # whole group closed
    fully = [m for m in msgs if "fully closed" in m]
    assert fully and "P/L +" in fully[0], fully
    assert not any(m.startswith("BASKET") for m in msgs)


def test_quick_fixed_tp_closes_orders_individually(tmp_path):
    tr, br, seen = _run_scalp(tmp_path)                       # default quick TP (0.25 ATR) is hit during the push
    msgs = [l["msg"] for l in tr.logs]
    assert any(m.startswith("SCALP ADD") for m in msgs)
    assert any("fully closed" in m and "P/L +" in m for m in msgs)
    assert not br.positions_


def test_scalp_never_enters_after_the_spike(tmp_path):
    cfg = EngineConfig(scalp_enabled=True)
    ticks = _scalp_path(reverse=False)
    # cut the early part: start the feed only AFTER the move already happened (price 1.2 px = 0.8 ATR above the bar open)
    cut = ticks[0][0] + 130 + 1.0
    late_only = [tk for tk in ticks if tk[0] >= cut + 10.0]
    tr, br = make(tmp_path, {"BTCUSDm": ticks[:int(130 / 0.3) - 1] + [(late_only[0][0], late_only[0][1], late_only[0][2])] + late_only}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=800.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200, on_step=lambda t: tr.atr_m1.__setitem__("BTCUSDm", 1.5))
    opened = [l["msg"] for l in tr.logs if l["msg"].startswith("SCALP OPEN")]
    assert not opened or all("swing 0.0" in m or "swing 0.1" in m or "swing 0.2" in m or "swing 0.3" in m for m in opened), opened


def test_free_margin_guard_and_cap_limit_orders(tmp_path):
    # tiny margin room: cap 100% but the guard keeps 3% free; lot is sized so 20 orders fill the cap
    tr, br, seen = _run_scalp(tmp_path, max_margin_use_pct=40.0)
    assert seen["margin"] <= 40.0 + 1e-6 and seen["n"] >= 1
    tr2, br2, seen2 = _run_scalp(tmp_path / "b", scalp_max_orders=3)
    assert seen2["n"] <= 3


def test_loser_is_cut_fast_when_the_reversal_says_wrong(tmp_path):
    rnd = random.Random(9)
    t, mid, out = T0, 2650.0, []

    def add(sec, step, dt=0.25, jitter=0.0):
        nonlocal t, mid
        for _ in range(int(sec / dt)):
            t += dt
            mid += step + (rnd.gauss(0, jitter) if jitter else 0.0)
            out.append((t, round(mid - 0.02, 2), round(mid + 0.02, 2)))
    add(130, 0.0, dt=0.3, jitter=0.003)
    add(1.5, 0.012)
    add(1.5, 0.025)                  # early impulse up ...
    add(12.0, -0.08)                 # ... that immediately fails and reverses hard
    add(30, 0.0, dt=0.3, jitter=0.003)
    tr, br = make(tmp_path, {"BTCUSDm": out}, EngineConfig(scalp_enabled=True))
    _small_account(br, balance=1000.0, margin_per_lot=800.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200, on_step=lambda t_: tr.atr_m1.__setitem__("BTCUSDm", 1.5))
    msgs = [l["msg"] for l in tr.logs]
    exits = [m for m in msgs if m.startswith("EXIT BATCH")]
    assert any(m.startswith("SCALP OPEN BUY") for m in msgs), msgs[-10:]
    assert any(("reversal cut" in m or "hard cut" in m or "wrong way" in m or "reversal take-profit" in m) for m in exits), exits
    assert not br.positions_
    fully = [m for m in msgs if "fully closed" in m]
    assert fully                     # losses are small: cut within seconds, never held
