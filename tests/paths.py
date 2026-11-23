"""Synthetic tick paths shared by tests (SYNTHETIC data, test double only)."""
import random

from tests.harness import now_base

T0 = now_base() // 60 * 60      # minute-aligned: live-candle features depend on the M1 phase


def _quiet(t, mid, secs, rnd):
    out = []
    end = t + secs
    while t < end:
        t += 0.3
        mid += rnd.gauss(0, 0.03)
        out.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return out, t, mid


def _leg(t, mid, secs, step, out, dt=0.25):
    end = t + secs
    while t < end:
        t += dt
        mid += step
        out.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return t, mid


def trend_then_pullback(seed=4):
    rnd = random.Random(seed)
    out, t, mid = _quiet(T0, 2650.0, 150, rnd)
    t, mid = _leg(t, mid, 30, +0.05, out)
    t, mid = _leg(t, mid, 14, -0.05, out)
    more, t, mid = _quiet(t, mid, 90, rnd)
    return out + more


def spike_then_fade(seed=4):
    rnd = random.Random(seed)
    out, t, mid = _quiet(T0, 2650.0, 150, rnd)
    t, mid = _leg(t, mid, 3, +0.06, out)
    t, mid = _leg(t, mid, 25, -0.012, out)
    more, t, mid = _quiet(t, mid, 60, rnd)
    return out + more


def _no_tp(br):
    orig = br.market_order
    br.market_order = lambda s, side, v, sl, tp, dev, magic, c: orig(s, side, v, sl, 0.0, dev, magic, c)


def _sl_track(br, hist):
    def on(t):
        for p in br.positions_.values():
            hist.setdefault(p["ticket"], []).append((p["type"], p["price_open"], p["sl"]))
    return on


# ------------------------------------------------------------- detector


def _small_account(br, balance=10.0, margin_per_lot=45.0):
    """Small account at high leverage: tiny margin per min lot, like Exness 1:2000."""
    br.balance = balance
    orig_acct = br.account

    def account():
        a = orig_acct()
        if a:
            used = sum(p["volume"] * margin_per_lot for p in br.positions_.values())
            a["margin_free"] = round(a["equity"] - used, 2)
            a["margin_level"] = (a["equity"] / used * 100.0) if used else 0.0
        return a
    br.account = account
    br.calc_margin = lambda side, s, vol, price: vol * margin_per_lot


def series_up(seed=3, start=None):
    """Quiet 150 s, an 8 s up-burst (ticks every 0.25 s), gentle continuation, then flat."""
    from tests.synth import burst_series
    s0 = T0 if start is None else start
    ticks = burst_series(s0, +1, pre_seconds=150, burst_seconds=8, step=0.08, seed=seed)
    t, mid = ticks[-1][0], (ticks[-1][1] + ticks[-1][2]) / 2
    for i in range(1, 900):
        t += 0.3
        mid += 0.02 if i < 60 else 0.0
        ticks.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return ticks


_series_up = series_up


def _start(tr, br, t):
    br.advance(t)
    tr.step(t)
    assert tr.conn_state == "CONNECTED", tr.conn_msg
    r = tr.cmd_start()
    assert r["ok"], r
    return r


def _force_signal(tr, side="BUY", origin=12345.0):
    real = tr._micro

    def fake(s, now_wall):
        m, st = real(s, now_wall)
        if m.ready:
            st.side, st.origin, st.reason = side, origin, f"micro-burst {side} (forced test signal)"
            if side == "BUY":
                st.buy = max(st.buy, 0.9)
            else:
                st.sell = max(st.sell, 0.9)
        return m, st
    tr._micro = fake
