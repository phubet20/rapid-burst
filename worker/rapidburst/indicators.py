"""Indicator confluence layer (t32u) from REAL broker M1 candles + the forming bar built from live ticks.

Computed once per closed M1 bar (cached) and read per tick with the live price:
  ribbon (EMA 8/13/21/34/55 order), session VWAP, Bollinger(20,2) / Keltner(20,1.5 ATR) squeeze + breakout,
  ADX/DI(14), Stochastic RSI(14,14,3), Supertrend(10,3), ATR regime (ATR14 vs ATR50), OBV slope (tick volume),
  Donchian(20) break, Heikin-Ashi trend, pivot support/resistance proximity.
Each indicator gives a vote in [-1, 1] (+ = up). The engine turns the weighted votes into family F, plus two
vetoes: entering straight into nearby support/resistance, and chop (low ADX / squeeze without a break).
Weights are learned per symbol from real closed baskets (learner.py, persisted, never decays).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

NAMES = ("ribbon", "vwap", "bb_break", "adx", "stochrsi", "supertrend", "obv", "donchian", "heikin", "atr_regime")


def _ema_series(xs: list, n: int) -> list:
    a, out, e = 2.0 / (n + 1.0), [], None
    for x in xs:
        e = x if e is None else e + a * (x - e)
        out.append(e)
    return out


def _wilder(xs: list, n: int) -> list:
    out, s = [], None
    for i, x in enumerate(xs):
        if s is None:
            s = x
        else:
            k = min(i + 1, n)
            s += (x - s) / k
        out.append(s)
    return out


def _rsi_series(closes: list, n: int = 14) -> list:
    ag = al = 0.0
    out = [50.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        k = min(i, n)
        ag += (max(ch, 0.0) - ag) / k
        al += (max(-ch, 0.0) - al) / k
        out.append(100.0 - 100.0 / (1.0 + ag / al) if al > 0 else 100.0)
    return out


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


@dataclass
class IndState:
    ready: bool = False
    t_last: float = 0.0
    atr: float = 0.0
    atr50: float = 0.0
    emas: dict = field(default_factory=dict)          # n -> last closed EMA
    bb_mid: float = 0.0
    bb_up: float = 0.0
    bb_lo: float = 0.0
    kc_up: float = 0.0
    kc_lo: float = 0.0
    squeeze_on: bool = False
    squeeze_recent: bool = False                       # squeeze in the last 5 closed bars
    adx: float = 0.0
    pdi: float = 0.0
    mdi: float = 0.0
    stoch_k: float = 50.0
    stoch_d: float = 50.0
    st_dir: int = 0
    st_line: float = 0.0
    obv_slope: float = 0.0
    don_hi: float = 0.0
    don_lo: float = 0.0
    ha_dir: int = 0
    vwap: float = 0.0
    levels: list = field(default_factory=list)         # pivot S/R prices


def compute(bars: list, n_min: int = 30) -> IndState:
    """Closed-bar state. bars: oldest -> newest dicts with time/open/high/low/close/vol."""
    st = IndState()
    if len(bars) < n_min:
        return st
    o = [b["open"] for b in bars]
    h = [b["high"] for b in bars]
    lo = [b["low"] for b in bars]
    c = [b["close"] for b in bars]
    v = [float(b.get("vol") or b.get("own") or 1.0) for b in bars]
    tr = [h[0] - lo[0]] + [max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1])) for i in range(1, len(c))]
    atr14 = _wilder(tr, 14)
    st.atr, st.atr50 = atr14[-1], _wilder(tr, 50)[-1]
    for n in (8, 13, 21, 34, 55):
        st.emas[n] = _ema_series(c, n)[-1]
    w = c[-20:]
    mid = sum(w) / len(w)
    sd = math.sqrt(sum((x - mid) ** 2 for x in w) / len(w))
    st.bb_mid, st.bb_up, st.bb_lo = mid, mid + 2 * sd, mid - 2 * sd
    e20 = _ema_series(c, 20)
    st.kc_up, st.kc_lo = e20[-1] + 1.5 * st.atr, e20[-1] - 1.5 * st.atr
    st.squeeze_on = st.bb_up < st.kc_up and st.bb_lo > st.kc_lo
    sq = []
    for j in range(max(20, len(c) - 5), len(c)):
        ww = c[j - 19:j + 1]
        mm = sum(ww) / 20.0
        ss = math.sqrt(sum((x - mm) ** 2 for x in ww) / 20.0)
        sq.append(mm + 2 * ss < e20[j] + 1.5 * atr14[j] and mm - 2 * ss > e20[j] - 1.5 * atr14[j])
    st.squeeze_recent = any(sq)
    # ADX / DI (Wilder 14)
    pdm = [0.0] + [max(h[i] - h[i - 1], 0.0) if (h[i] - h[i - 1]) > (lo[i - 1] - lo[i]) else 0.0 for i in range(1, len(c))]
    mdm = [0.0] + [max(lo[i - 1] - lo[i], 0.0) if (lo[i - 1] - lo[i]) > (h[i] - h[i - 1]) else 0.0 for i in range(1, len(c))]
    trs, ps, ms = _wilder(tr, 14), _wilder(pdm, 14), _wilder(mdm, 14)
    dx = []
    for i in range(len(c)):
        p_ = 100 * ps[i] / trs[i] if trs[i] else 0.0
        m_ = 100 * ms[i] / trs[i] if trs[i] else 0.0
        dx.append(100 * abs(p_ - m_) / (p_ + m_) if (p_ + m_) else 0.0)
        if i == len(c) - 1:
            st.pdi, st.mdi = p_, m_
    st.adx = _wilder(dx, 14)[-1]
    # Stochastic RSI (14, 14, 3)
    r = _rsi_series(c, 14)
    ks = []
    for i in range(len(r)):
        win = r[max(0, i - 13):i + 1]
        hi_, lo_ = max(win), min(win)
        ks.append(100 * (r[i] - lo_) / (hi_ - lo_) if hi_ > lo_ else 50.0)
    k3 = [sum(ks[max(0, i - 2):i + 1]) / len(ks[max(0, i - 2):i + 1]) for i in range(len(ks))]
    st.stoch_k = k3[-1]
    st.stoch_d = sum(k3[-3:]) / len(k3[-3:])
    # Supertrend (10, 3)
    a10 = _wilder(tr, 10)
    up_b = lo_b = 0.0
    d_ = 1
    for i in range(len(c)):
        hl2 = (h[i] + lo[i]) / 2
        bu, bl = hl2 + 3 * a10[i], hl2 - 3 * a10[i]
        if i == 0:
            up_b, lo_b = bu, bl
            continue
        up_b = bu if (bu < up_b or c[i - 1] > up_b) else up_b
        lo_b = bl if (bl > lo_b or c[i - 1] < lo_b) else lo_b
        if d_ == 1 and c[i] < lo_b:
            d_ = -1
        elif d_ == -1 and c[i] > up_b:
            d_ = 1
    st.st_dir, st.st_line = d_, (lo_b if d_ == 1 else up_b)
    # OBV slope over 10 bars, normalised by average volume
    obv = [0.0]
    for i in range(1, len(c)):
        obv.append(obv[-1] + (v[i] if c[i] > c[i - 1] else -v[i] if c[i] < c[i - 1] else 0.0))
    avgv = sum(v[-20:]) / len(v[-20:]) or 1.0
    st.obv_slope = _clip((obv[-1] - obv[-11]) / (10 * avgv)) if len(obv) > 11 else 0.0
    st.don_hi, st.don_lo = max(h[-20:]), min(lo[-20:])
    # Heikin-Ashi
    ha_o, ha_c, cols = (o[0] + c[0]) / 2, (o[0] + h[0] + lo[0] + c[0]) / 4, []
    for i in range(1, len(c)):
        ha_o = (ha_o + ha_c) / 2
        ha_c = (o[i] + h[i] + lo[i] + c[i]) / 4
        cols.append(1 if ha_c > ha_o else -1)
    st.ha_dir = cols[-1] if len(cols) >= 2 and cols[-1] == cols[-2] else 0
    # session VWAP (UTC day of the last bar; falls back to the loaded window)
    day = bars[-1]["time"] - bars[-1]["time"] % 86400
    sess = [i for i, b in enumerate(bars) if b["time"] >= day] or list(range(len(bars)))
    pv = sum((h[i] + lo[i] + c[i]) / 3 * v[i] for i in sess)
    vv = sum(v[i] for i in sess)
    st.vwap = pv / vv if vv else c[-1]
    # pivot S/R: 2-bar fractals of the loaded window
    lv = []
    for i in range(2, len(c) - 2):
        if h[i] >= max(h[i - 2], h[i - 1], h[i + 1], h[i + 2]):
            lv.append(h[i])
        if lo[i] <= min(lo[i - 2], lo[i - 1], lo[i + 1], lo[i + 2]):
            lv.append(lo[i])
    st.levels = lv[-40:]
    st.t_last = bars[-1]["time"]
    st.ready = st.atr > 0
    return st


@dataclass
class IndView:
    ready: bool = False
    votes: dict = field(default_factory=dict)          # name -> vote (+ up / - down)
    chop: bool = False
    chop_why: str = ""
    res_above: Optional[float] = None                  # nearest resistance above / support below (price units)
    sup_below: Optional[float] = None
    breakout: int = 0                                  # +1 / -1 Donchian or squeeze breakout in progress
    adx: float = 0.0

    def score(self, d: int, weights: Optional[dict] = None) -> float:
        """0..1 confluence for direction d (0.5 = neutral)."""
        if not self.ready or not self.votes:
            return 0.5
        w = weights or {}
        num = sum(w.get(k, 1.0) * d * x for k, x in self.votes.items())
        den = sum(w.get(k, 1.0) for k in self.votes) or 1.0
        return _clip(0.5 + 0.5 * num / den, 0.0, 1.0)

    def brief(self) -> dict:
        return {"ready": self.ready, "votes": {k: round(x, 2) for k, x in self.votes.items()}, "chop": self.chop,
                "chop_why": self.chop_why, "breakout": self.breakout, "adx": round(self.adx, 1),
                "res_above": self.res_above, "sup_below": self.sup_below}


def view(st: IndState, price: float, live: Optional[dict]) -> IndView:
    iv = IndView()
    if not st.ready or price <= 0:
        return iv
    atr = st.atr
    iv.ready, iv.adx = True, st.adx
    e = st.emas
    seq = [e[8], e[13], e[21], e[34], e[55]]
    up = sum(1 for a, b in zip(seq, seq[1:]) if a > b)
    iv.votes["ribbon"] = (up - (4 - up)) / 4.0 * (1.0 if (price - e[21]) * (up - 2) >= 0 else 0.5)
    iv.votes["vwap"] = _clip((price - st.vwap) / (1.5 * atr))
    brk = 0
    if price > st.don_hi:
        brk = 1
    elif price < st.don_lo:
        brk = -1
    if st.squeeze_recent and (price > st.bb_up or price < st.bb_lo):
        brk = 1 if price > st.bb_up else -1
    iv.breakout = brk
    iv.votes["bb_break"] = float(brk) if brk else _clip((price - st.bb_mid) / max(st.bb_up - st.bb_mid, 1e-12)) * 0.4
    iv.votes["donchian"] = float(brk) if brk else _clip(((price - st.don_lo) / max(st.don_hi - st.don_lo, 1e-12)) * 2 - 1) * 0.3
    di = 1.0 if st.pdi > st.mdi else -1.0
    iv.votes["adx"] = di * _clip((st.adx - 18.0) / 20.0, 0.0, 1.0)
    k, dd = st.stoch_k, st.stoch_d
    sv = (1.0 if k > dd else -1.0) * 0.6
    if k > 90:
        sv -= 0.5
    elif k < 10:
        sv += 0.5
    iv.votes["stochrsi"] = _clip(sv)
    iv.votes["supertrend"] = float(st.st_dir) if (price - st.st_line) * st.st_dir > 0 else -0.5 * st.st_dir
    iv.votes["obv"] = st.obv_slope
    iv.votes["heikin"] = float(st.ha_dir)
    reg = st.atr / st.atr50 if st.atr50 else 1.0
    trend_sign = 1.0 if (iv.votes["ribbon"] + iv.votes["supertrend"]) > 0 else -1.0
    iv.votes["atr_regime"] = trend_sign * _clip((reg - 1.0) / 0.4, 0.0, 1.0)
    above = [x for x in st.levels if x > price]
    below = [x for x in st.levels if x < price]
    iv.res_above = min(above) if above else None
    iv.sup_below = max(below) if below else None
    if st.adx < 18.0 and not brk:
        iv.chop, iv.chop_why = True, f"ADX {st.adx:.0f} < 18 (no trend) and no breakout"
    elif st.squeeze_on and not brk:
        iv.chop, iv.chop_why = True, "Bollinger inside Keltner (squeeze) - waiting for the break"
    return iv


def sr_block(iv: IndView, d: int, price: float, atr: float, near_atr: float) -> Optional[str]:
    """Entering straight into nearby resistance (BUY) / support (SELL) without a breakout."""
    if not iv.ready or atr <= 0 or iv.breakout == d:
        return None
    if d > 0 and iv.res_above is not None and iv.res_above - price < near_atr * atr:
        return f"resistance {iv.res_above:.5g} only {(iv.res_above - price) / atr:.2f} ATR above"
    if d < 0 and iv.sup_below is not None and price - iv.sup_below < near_atr * atr:
        return f"support {iv.sup_below:.5g} only {(price - iv.sup_below) / atr:.2f} ATR below"
    return None
