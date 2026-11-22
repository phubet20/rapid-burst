"""Higher-timeframe trend filter (pure logic).

Built from broker M1/M5 bars (TickerAll GET /candles or MT5 copy_rates). It
answers "which way is the main trend and how strong is it" so entries can be
biased with it. Inputs are real bars; with too few bars the trend is reported
as 'unknown' (never guessed) and entries fall back to the momentum rules alone.

Strength blends three normalised readings (each in [-1, 1]):
  * M1 regression slope over the last 15 bars  / M1 average bar range
  * M1 EMA(8) - EMA(21)                         / M1 average bar range
  * M5 regression slope over the last 12 bars  / M5 average bar range
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Trend:
    dir: int = 0              # +1 up, -1 down, 0 flat / unknown
    strength: float = 0.0     # 0..1
    label: str = "unknown"
    known: bool = False
    detail: dict = field(default_factory=dict)

    def brief(self) -> dict:
        return {"dir": self.dir, "strength": round(self.strength, 2), "label": self.label, "known": self.known,
                **{k: round(v, 3) for k, v in self.detail.items()}}


def _slope(ys: list) -> float:
    n = len(ys)
    if n < 3:
        return 0.0
    mx = (n - 1) / 2.0
    my = sum(ys) / n
    num = sum((i - mx) * (y - my) for i, y in enumerate(ys))
    den = sum((i - mx) ** 2 for i in range(n))
    return num / den if den else 0.0


def _ema(ys: list, n: int) -> float:
    k = 2.0 / (n + 1)
    e = ys[0]
    for y in ys[1:]:
        e = y * k + e * (1 - k)
    return e


def _avg_range(bars: list) -> float:
    r = [float(b["high"]) - float(b["low"]) for b in bars if b.get("high") is not None and b.get("low") is not None]
    r = [x for x in r if x > 0]
    if r:
        return sum(r) / len(r)
    c = [float(b["close"]) for b in bars]
    d = [abs(c[i] - c[i - 1]) for i in range(1, len(c))]
    return sum(d) / len(d) if d else 0.0


def compute_trend(m1: list, m5: list, min_strength: float = 0.25) -> Trend:
    """m1/m5: lists of bars {open, high, low, close} oldest -> newest."""
    parts, detail = [], {}
    if len(m1) >= 21:
        tail = m1[-30:]
        atr = _avg_range(tail[-14:])
        closes = [float(b["close"]) for b in tail]
        if atr > 0:
            s1 = _slope(closes[-15:]) / atr
            e = (_ema(closes, 8) - _ema(closes, 21)) / atr
            detail.update(m1_slope=s1, m1_ema_gap=e)
            parts += [(0.4, math.tanh(s1 / 0.3)), (0.3, math.tanh(e / 1.0))]
    if len(m5) >= 12:
        tail = m5[-12:]
        atr = _avg_range(tail)
        if atr > 0:
            s5 = _slope([float(b["close"]) for b in tail]) / atr
            detail.update(m5_slope=s5)
            parts.append((0.3, math.tanh(s5 / 0.3)))
    if not parts:
        return Trend(label="unknown (not enough bars)", detail=detail)
    wsum = sum(w for w, _ in parts)
    score = sum(w * v for w, v in parts) / wsum
    strength = min(1.0, abs(score))
    d = (1 if score > 0 else -1) if strength >= min_strength else 0
    label = ("up" if d > 0 else "down" if d < 0 else "flat") + f" {strength:.2f}"
    return Trend(dir=d, strength=strength, label=label, known=True, detail=detail)


def relation(trend: Optional[Trend], side: str) -> str:
    """aligned | counter | neutral (flat or unknown trend)."""
    if trend is None or not trend.known or trend.dir == 0:
        return "neutral"
    return "aligned" if (trend.dir > 0) == (side == "BUY") else "counter"
