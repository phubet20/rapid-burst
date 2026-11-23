"""t37u: price-action context from REAL broker candles (M5 main frame, M1 timing) - used as SCORE INPUTS, never as a
single deciding indicator:

* demand / supply zones (DM/SP): the base candle(s) right before an impulsive move (body >= impulse_atr * ATR);
  a zone stays valid until price closes through it. Price inside / just above a fresh demand zone supports BUY,
  inside / just below a supply zone supports SELL.
* fair value gaps (FVG): 3-candle imbalance (bar[i-2].high < bar[i].low = bullish gap, bar[i-2].low > bar[i].high =
  bearish gap), valid until filled. Price retracing INTO an unfilled bullish FVG supports BUY (and vice versa).
* confirmation candle: the last CLOSED bar closes strongly in the direction (body >= 50% of range, close in the
  outer 30%) or engulfs the previous bar.
* rejection / wick tip: the forming bar printed a long wick against the direction and price is still near that wick
  tip (BUY near the low wick of a down-spike, SELL near the high wick of an up-spike) - an entry at the wick tip,
  before the bar bounces, instead of chasing the bounce.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


@dataclass
class Zone:
    kind: str          # "demand" | "supply" | "fvg+" | "fvg-"
    lo: float
    hi: float
    t: float


@dataclass
class ZoneView:
    ready: bool = False
    zones: list = field(default_factory=list)
    confirm: int = 0          # +1 bullish confirmation candle, -1 bearish
    wick: int = 0             # +1 price at the tip of a long lower wick (BUY), -1 at an upper wick tip (SELL)
    tags: dict = field(default_factory=lambda: {"BUY": [], "SELL": []})
    scores: dict = field(default_factory=lambda: {"BUY": 0.5, "SELL": 0.5})

    def score(self, d: int) -> float:
        return self.scores["BUY" if d > 0 else "SELL"]

    def holds(self, d: int) -> bool:
        """Price is pulling back INTO a zone that supports the held side (demand/bull-FVG for BUY) = a pullback,
        not a reversal."""
        return any(t in ("demand", "fvg+") for t in self.tags["BUY"]) if d > 0 else \
            any(t in ("supply", "fvg-") for t in self.tags["SELL"])

    def brief(self) -> dict:
        return {"ready": self.ready, "confirm": self.confirm, "wick": self.wick,
                "buy": round(self.scores["BUY"], 2), "sell": round(self.scores["SELL"], 2),
                "tags": self.tags, "zones": [(z.kind, round(z.lo, 5), round(z.hi, 5)) for z in self.zones[-6:]]}


def _atr(bars: list, n: int = 14) -> float:
    rs = [b["high"] - b["low"] for b in bars[-n:]]
    return sum(rs) / len(rs) if rs else 0.0


def find_zones(bars: list, atr: float, impulse_atr: float = 1.2, max_zones: int = 8) -> list:
    out: list = []
    if len(bars) < 5 or atr <= 0:
        return out
    for i in range(2, len(bars)):
        b, p = bars[i], bars[i - 1]
        body = b["close"] - b["open"]
        if abs(body) >= impulse_atr * atr:                       # impulsive candle -> the base before it is a zone
            if body > 0:
                out.append(Zone("demand", p["low"], max(p["open"], p["close"]), float(p["time"])))
            else:
                out.append(Zone("supply", min(p["open"], p["close"]), p["high"], float(p["time"])))
        a = bars[i - 2]
        if a["high"] < b["low"]:
            out.append(Zone("fvg+", a["high"], b["low"], float(b["time"])))
        elif a["low"] > b["high"]:
            out.append(Zone("fvg-", b["high"], a["low"], float(b["time"])))
    valid = []
    for z in out:                                                # invalidate zones price has since closed through
        later = [x for x in bars if float(x["time"]) > z.t]
        if z.kind == "demand" and any(x["close"] < z.lo for x in later):
            continue
        if z.kind == "supply" and any(x["close"] > z.hi for x in later):
            continue
        if z.kind == "fvg+" and any(x["low"] <= z.lo for x in later[1:]):
            continue
        if z.kind == "fvg-" and any(x["high"] >= z.hi for x in later[1:]):
            continue
        valid.append(z)
    return valid[-max_zones:]


def confirmation(bars: list) -> int:
    if len(bars) < 2:
        return 0
    b, p = bars[-1], bars[-2]
    rng = b["high"] - b["low"]
    if rng <= 0:
        return 0
    body = b["close"] - b["open"]
    loc = (b["close"] - b["low"]) / rng
    if body > 0 and (abs(body) >= 0.5 * rng and loc >= 0.7 or (b["close"] > p["high"] and b["open"] <= p["close"])):
        return 1
    if body < 0 and (abs(body) >= 0.5 * rng and loc <= 0.3 or (b["close"] < p["low"] and b["open"] >= p["close"])):
        return -1
    return 0


def wick_tip(live: Optional[dict], price: float, atr: float, min_wick_atr: float = 0.35, near_frac: float = 0.35) -> int:
    """+1 = long LOWER wick on the forming bar and price still in the lower part of it (buy the wick tip)."""
    if not live or atr <= 0:
        return 0
    o, h, lo_ = float(live["open"]), float(live["high"]), float(live["low"])
    body_lo, body_hi = min(o, price), max(o, price)
    low_w, up_w = body_lo - lo_, h - body_hi
    if low_w >= min_wick_atr * atr and low_w > 1.5 * up_w and price - lo_ <= near_frac * max(h - lo_, 1e-12) + 0.25 * low_w:
        return 1
    if up_w >= min_wick_atr * atr and up_w > 1.5 * low_w and h - price <= near_frac * max(h - lo_, 1e-12) + 0.25 * up_w:
        return -1
    return 0


def analyze(m5: list, m1: list, live: Optional[dict], price: float, atr: float = 0.0, cfg=None) -> ZoneView:
    v = ZoneView()
    frame = m5 if len(m5) >= 10 else m1
    if len(frame) < 5 or price <= 0:
        return v
    a = _atr(frame) or atr
    if a <= 0:
        return v
    imp = float(getattr(cfg, "zone_impulse_atr", 1.2) or 1.2)
    v.zones = find_zones(frame, a, imp)
    if m1 and frame is not m1:
        v.zones += [z for z in find_zones(m1, _atr(m1) or a, imp) if z.kind.startswith("fvg")][-4:]
    v.ready = True
    near = 0.15 * a
    for z in v.zones:
        inside = z.lo - near <= price <= z.hi + near
        if not inside:
            continue
        if z.kind in ("demand", "fvg+"):
            v.tags["BUY"].append(z.kind)
        else:
            v.tags["SELL"].append(z.kind)
    v.confirm = confirmation(m1 if m1 else frame)
    a1 = _atr(m1) if m1 else a
    v.wick = wick_tip(live, price, a1 or a)
    for d, side in ((1, "BUY"), (-1, "SELL")):
        s = 0.5
        s += 0.15 * min(2, len(v.tags[side]))                      # in a supporting zone / FVG
        opp = "SELL" if d > 0 else "BUY"
        s -= 0.10 * min(2, len(v.tags[opp]))                       # trading INTO an opposing zone
        s += 0.12 * (v.confirm == d) - 0.08 * (v.confirm == -d)
        s += 0.15 * (v.wick == d) - 0.10 * (v.wick == -d)
        v.scores[side] = _clip(s)
    return v
