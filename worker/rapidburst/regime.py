"""t40u: the market state the bot remembers - only UP, DOWN or SIDEWAYS - plus a PHASE counter of the current trend
(impulse / pullback legs numbered 1, 2, 3 ...) and the phase expected next. Built from REAL broker M5 bars plus the
forming M5 bar (no close wait), confirmed by M1.

Swings: a pivot high/low = a bar whose high/low is the extreme of +-`k` bars (the forming bar may complete the last
pivot). UP = the last two swing highs AND lows rising (HH + HL), DOWN = both falling (LH + LL), anything else - or a
range narrower than `range_atr` ATR / ADX below `adx_min` - is SIDEWAYS.
Phases: counted from the swing where the trend started; odd phase = impulse leg (with the trend), even phase =
pullback leg. Expected next: impulse -> pullback, pullback -> continuation impulse, after `max_phase` phases or a
structure break -> reversal / sideways risk.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RegimeView:
    ready: bool = False
    state: str = "SIDEWAYS"          # UP | DOWN | SIDEWAYS
    phase: int = 0                   # 1, 2, 3 ... legs since the trend started (0 = no trend)
    leg: str = "-"                   # "impulse" | "pullback" | "range"
    expect: str = "-"                # what the next phase should be
    origin: float = 0.0              # pullback origin (last swing low in UP / swing high in DOWN) = anti-chase anchor
    range_hi: float = 0.0
    range_lo: float = 0.0
    atr: float = 0.0
    why: str = ""
    swings: list = field(default_factory=list)

    @property
    def dir(self) -> int:
        return 1 if self.state == "UP" else -1 if self.state == "DOWN" else 0

    def label(self) -> str:
        if not self.ready:
            return "regime: waiting for bars"
        if self.state == "SIDEWAYS":
            return f"SIDEWAYS (range {self.range_lo:.5g}-{self.range_hi:.5g}) - fade the edges at wicks"
        return f"{self.state} phase {self.phase} ({self.leg}) -> next: {self.expect}"

    def brief(self) -> dict:
        return {"ready": self.ready, "state": self.state, "phase": self.phase, "leg": self.leg, "expect": self.expect,
                "origin": self.origin, "range_hi": self.range_hi, "range_lo": self.range_lo, "why": self.why,
                "label": self.label()}


def _atr(bars: list, n: int = 14) -> float:
    rs = [b["high"] - b["low"] for b in bars[-n:]]
    return sum(rs) / len(rs) if rs else 0.0


def swings(bars: list, k: int = 2) -> list:
    """[(index, 'H'|'L', price)] alternating pivots."""
    out: list = []
    n = len(bars)
    for i in range(k, n):
        lo_i, hi_i = max(0, i - k), min(n, i + k + 1)
        win = bars[lo_i:hi_i]
        h, l_ = bars[i]["high"], bars[i]["low"]
        if h >= max(b["high"] for b in win):
            if out and out[-1][1] == "H":
                if h >= out[-1][2]:
                    out[-1] = (i, "H", h)
            else:
                out.append((i, "H", h))
        if l_ <= min(b["low"] for b in win):
            if out and out[-1][1] == "L":
                if l_ <= out[-1][2]:
                    out[-1] = (i, "L", l_)
            else:
                out.append((i, "L", l_))
    return out


def analyze(m5: list, live: Optional[dict] = None, adx: float = 0.0, cfg=None) -> RegimeView:
    v = RegimeView()
    bars = list(m5) + ([dict(live)] if live and live.get("high") is not None else [])
    if len(bars) < 12:
        v.why = f"{len(bars)} M5 bars"
        return v
    v.ready = True
    k = int(getattr(cfg, "regime_pivot_k", 2) or 2)
    max_phase = int(getattr(cfg, "regime_max_phase", 5) or 5)
    adx_min = float(getattr(cfg, "regime_adx_min", 15.0) or 0.0)
    v.atr = a = _atr(bars) or 1e-9
    sw = swings(bars[-80:], k)
    v.swings = sw[-8:]
    highs = [p for _, t, p in sw if t == "H"]
    lows = [p for _, t, p in sw if t == "L"]
    recent = bars[-24:]
    v.range_hi, v.range_lo = max(b["high"] for b in recent), min(b["low"] for b in recent)
    price = bars[-1]["close"]
    if len(highs) < 2 or len(lows) < 2:
        v.state, v.leg, v.expect, v.why = "SIDEWAYS", "range", "breakout or more swings", "not enough swings"
        return v
    up = highs[-1] > highs[-2] and lows[-1] > lows[-2]
    dn = highs[-1] < highs[-2] and lows[-1] < lows[-2]
    # structure break against the last swing flips the state immediately (no close wait: the forming bar counts)
    if up and price < lows[-1]:
        up, v.why = False, "UP structure broken (price below the last higher low)"
    if dn and price > highs[-1]:
        dn, v.why = False, "DOWN structure broken (price above the last lower high)"
    if (up or dn) and adx and adx_min and adx < adx_min and (v.range_hi - v.range_lo) < 3.0 * a:
        up = dn = False
        v.why = f"ADX {adx:.0f} < {adx_min:g} in a {((v.range_hi - v.range_lo) / a):.1f} ATR range"
    if not (up or dn):
        v.state, v.leg = "SIDEWAYS", "range"
        v.expect = "fade the range edges at wicks until a breakout"
        v.why = v.why or "swings not aligned (no HH/HL or LH/LL)"
        return v
    d = 1 if up else -1
    v.state = "UP" if up else "DOWN"
    # trend start = the last swing that broke the opposite sequence; count legs (pivot to pivot) since then
    start = 0
    for j in range(len(sw) - 1, 1, -1):
        _, t, p = sw[j]
        prev_same = [q for _, tt, q in sw[:j] if tt == t]
        if not prev_same:
            break
        ok = (p > prev_same[-1]) if d > 0 else (p < prev_same[-1])
        if not ok:
            start = j
            break
    n_used = min(80, len(bars))
    last_i, last_t, _ = sw[-1]
    confirmed = last_i < n_used - k                       # a pivot on the last k bars is still forming
    moving_to = ("H" if last_t == "L" else "L") if confirmed else last_t
    # the leg running NOW: moving toward a new high in an up-trend = impulse, toward a low = pullback
    impulse_now = (moving_to == "H") if d > 0 else (moving_to == "L")
    done = (len(sw) - 1 - start) if confirmed else (len(sw) - 2 - start)
    phase = max(0, done) + 1
    if impulse_now and phase % 2 == 0:
        phase += 1
    if not impulse_now and phase % 2 == 1:
        phase += 1
    v.phase = phase
    v.leg = "impulse" if impulse_now else "pullback"
    v.origin = lows[-1] if d > 0 else highs[-1]
    if phase >= max_phase:
        v.expect = f"phase {phase} >= {max_phase}: exhaustion - reversal / sideways risk"
    elif impulse_now:
        v.expect = f"phase {phase + 1}: pullback (wait for the wick)"
    else:
        v.expect = f"phase {phase + 1}: continuation impulse {'up' if d > 0 else 'down'} - buy/sell the pullback wick"
    v.why = v.why or ("HH + HL" if d > 0 else "LH + LL")
    return v
