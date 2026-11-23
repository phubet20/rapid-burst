"""Tick data layer (pure logic, no MetaTrader dependency).

Per symbol: a rolling buffer of REAL ticks, the spread history, a short-term volatility unit (typical 3 s
move) and a few base measurements (velocity/acceleration in vol units, tick pressure, micro breakout,
compression/expansion) plus the spread/quality gate. Signal strength, entries and exits are decided in
ONE place: microburst.py. No RSI and no lagging oscillators.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import deque
from dataclasses import dataclass, field
from statistics import median
from typing import Optional

from .config import EngineConfig


@dataclass
class Tick:
    t: float      # seconds (epoch, ms precision)
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass
class Features:
    ready: bool = False
    reason: str = "warming up"
    t: float = 0.0
    mid: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    spread: float = 0.0
    vol_unit: float = 0.0
    tick_rate: float = 0.0
    d_fast: float = 0.0
    d_prev: float = 0.0
    accel: float = 0.0
    d_slow: float = 0.0
    pressure: float = 0.0
    breakout: int = 0
    compressed: bool = False
    expansion: float = 0.0
    score: float = 0.0           # filled with the micro-burst strength for display

    def brief(self) -> dict:
        r = lambda x: round(x, 3)
        return {
            "ready": self.ready, "reason": self.reason, "score": r(self.score),
            "vel": r(self.d_fast), "acc": r(self.accel), "slow": r(self.d_slow),
            "press": r(self.pressure), "brk": self.breakout, "comp": self.compressed,
            "exp": r(self.expansion), "spread": self.spread, "vu": self.vol_unit,
            "tps": r(self.tick_rate),
        }


@dataclass
class Signal:
    action: str            # BUY / SELL / HOLD
    score: float
    reason: str
    features: Features


class TickBuffer:
    """Rolling window of ticks with time-indexed lookups."""

    def __init__(self, horizon: float):
        self.horizon = horizon
        self.ticks: deque[Tick] = deque()
        self._times: list[float] = []
        self._dirty = True
        self.last_t = 0.0

    def add(self, tick: Tick) -> bool:
        if tick.t <= self.last_t and self.ticks and tick.bid == self.ticks[-1].bid and tick.ask == self.ticks[-1].ask:
            return False
        if tick.t < self.last_t:
            return False
        if tick.bid <= 0 or tick.ask <= 0 or tick.ask < tick.bid:
            return False
        self.ticks.append(tick)
        self.last_t = tick.t
        cutoff = tick.t - self.horizon
        while self.ticks and self.ticks[0].t < cutoff:
            self.ticks.popleft()
        self._dirty = True
        return True

    def __len__(self) -> int:
        return len(self.ticks)

    def times(self) -> list[float]:
        if self._dirty:
            self._times = [x.t for x in self.ticks]
            self._dirty = False
        return self._times

    def at(self, t: float) -> Optional[Tick]:
        """Last tick at or before time t."""
        ts = self.times()
        i = bisect_right(ts, t) - 1
        if i < 0:
            return None
        return self.ticks[i]

    def window(self, t0: float, t1: float) -> list[Tick]:
        ts = self.times()
        i0 = bisect_right(ts, t0)
        i1 = bisect_right(ts, t1)
        return [self.ticks[i] for i in range(i0, i1)]


def _range(ticks: list[Tick]) -> float:
    if not ticks:
        return 0.0
    m = [x.mid for x in ticks]
    return max(m) - min(m)


class MomentumEngine:
    """Per-symbol tick store + base features + spread gate (decisions live in microburst.py)."""

    def __init__(self, symbol: str, point: float, cfg: EngineConfig):
        self.symbol = symbol
        self.point = point if point > 0 else 1e-5
        self.cfg = cfg
        self.buf = TickBuffer(horizon=cfg.vol_window + 30.0)
        self.spreads: deque[float] = deque(maxlen=400)
        self.last: Features = Features()
        self.seed_vu = 0.0      # 3 s move scale seeded from broker M1 candles (used while ticks are few)

    # ------------------------------------------------------------------ ticks
    def on_tick(self, t: float, bid: float, ask: float) -> bool:
        ok = self.buf.add(Tick(t, bid, ask))
        if ok:
            self.spreads.append(ask - bid)
        return ok

    # --------------------------------------------------------------- features
    def _vol_unit(self, now: float) -> float:
        """Mean absolute fast-window move, sampled on a 1-second grid."""
        cfg = self.cfg
        start = max(self.buf.ticks[0].t, now - cfg.vol_window)
        grid = []
        t = start
        while t <= now:
            tk = self.buf.at(t)
            if tk is not None:
                grid.append(tk.mid)
            t += 1.0
        k = max(1, int(round(cfg.fast_window)))
        diffs = [abs(grid[i] - grid[i - k]) for i in range(k, len(grid))]
        vu = sum(diffs) / len(diffs) if diffs else 0.0
        if self.seed_vu > 0:
            w = min(1.0, max(0.0, (now - self.buf.ticks[0].t) / 30.0))   # blend candle seed -> live ticks
            vu = w * vu + (1.0 - w) * self.seed_vu if diffs else self.seed_vu
        return max(vu, 2.0 * self.point)

    def features(self, now: Optional[float] = None) -> Features:
        cfg = self.cfg
        f = Features()
        if not self.buf.ticks:
            f.reason = "no ticks"
            self.last = f
            return f
        last = self.buf.ticks[-1]
        now = last.t if now is None else now
        f.t, f.mid, f.bid, f.ask, f.spread = last.t, last.mid, last.bid, last.ask, last.spread
        span = last.t - self.buf.ticks[0].t
        if len(self.buf) < cfg.min_ticks_warmup or span < cfg.warmup_seconds:
            f.reason = (f"warming up: {len(self.buf)}/{cfg.min_ticks_warmup} ticks, "
                        f"{span:.0f}/{cfg.warmup_seconds:.0f}s of live data")
            self.last = f
            return f
        if now - last.t > cfg.max_tick_age:
            f.reason = f"market idle (last tick {now - last.t:.0f}s ago)"
            self.last = f
            return f
        recent = self.buf.window(last.t - 30.0, last.t)
        f.tick_rate = len(recent) / max(2.0, min(30.0, last.t - self.buf.ticks[0].t))   # span-aware: no 30 s wait
        vu = self._vol_unit(last.t)
        f.vol_unit = vu
        p_fast = self.buf.at(last.t - cfg.fast_window) or self.buf.ticks[0]
        p_prev = self.buf.at(last.t - 2 * cfg.fast_window) or self.buf.ticks[0]
        p_slow = self.buf.at(last.t - cfg.slow_window) or self.buf.ticks[0]
        f.d_fast = (last.mid - p_fast.mid) / vu
        f.d_prev = (p_fast.mid - p_prev.mid) / vu
        f.accel = f.d_fast - f.d_prev
        f.d_slow = (last.mid - p_slow.mid) / vu

        # pressure: tick-direction count imbalance + magnitude efficiency (last 5s)
        w = self.buf.window(last.t - 5.0, last.t)
        prev = self.buf.at(last.t - 5.0)
        seq = ([prev] if prev else []) + w
        up = dn = 0
        s_abs = s_net = 0.0
        for a, b in zip(seq, seq[1:]):
            d = b.mid - a.mid
            if d > 0:
                up += 1
            elif d < 0:
                dn += 1
            s_abs += abs(d)
            s_net += d
        cnt = (up - dn) / (up + dn) if up + dn else 0.0
        eff = s_net / s_abs if s_abs > 0 else 0.0
        f.pressure = 0.5 * cnt + 0.5 * eff

        # micro-structure: range of [now-lookback, now-fast] and breakout of it
        struct = self.buf.window(last.t - cfg.breakout_lookback, last.t - cfg.fast_window)
        if struct:
            hi = max(x.mid for x in struct)
            lo = min(x.mid for x in struct)
            buf = 0.1 * vu
            if last.mid > hi + buf:
                f.breakout = 1
            elif last.mid < lo - buf:
                f.breakout = -1
        # compression / expansion versus typical 5s block range
        blocks = []
        t0 = max(self.buf.ticks[0].t, last.t - cfg.vol_window)
        t = t0
        while t + 5.0 <= last.t:
            r = _range(self.buf.window(t, t + 5.0))
            if r > 0:
                blocks.append(r)
            t += 5.0
        r5 = median(blocks) if blocks else vu
        r5 = max(r5, self.point)
        pre = _range(self.buf.window(last.t - 15.0, last.t - cfg.fast_window))
        f.compressed = pre < 1.3 * r5
        fast_range = _range(self.buf.window(last.t - cfg.fast_window, last.t) + [p_fast])
        f.expansion = fast_range / r5

        f.ready = True
        f.reason = "ok"
        self.last = f
        return f

    # ------------------------------------------------------------- decisions
    def spread_ok(self, f: Features) -> tuple[bool, str]:
        cfg = self.cfg
        sp_pts = f.spread / self.point
        if cfg.max_spread_points > 0 and sp_pts > cfg.max_spread_points:
            return False, f"spread {sp_pts:.0f}pt > cap {cfg.max_spread_points:.0f}pt"
        bps = f.spread / f.mid * 1e4 if f.mid > 0 else 0.0
        if cfg.max_spread_bps > 0 and bps > cfg.max_spread_bps:
            return False, f"spread {bps:.1f}bp of price > cap {cfg.max_spread_bps:.1f}bp"
        if f.vol_unit > 0 and f.spread > cfg.max_spread_vol_ratio * f.vol_unit:
            return False, f"spread {sp_pts:.0f}pt too wide vs volatility"
        if len(self.spreads) > 30:
            med = median(self.spreads)
            if med > 0 and f.spread > cfg.spread_spike_ratio * med:
                return False, f"spread spike ({f.spread / med:.1f}x median)"
        return True, "ok"


@dataclass
class PositionState:
    ticket: int
    symbol: str
    side: str
    volume: float
    open_price: float
    open_time: float
    sl: float = 0.0
    tp: float = 0.0
    entry_vu: float = 0.0
    peak_units: float = 0.0
    reason: str = ""
    closing_attempts: int = 0
    batch_id: str = ""
    hard_dist: float = 0.0       # engine hard-close distance (price units); software backup stop
    sl_state: str = "initial"    # initial -> BE+ -> trail
    vsl: float = 0.0             # software mirror of the protective level (used if a broker modify fails)
    last_mod: float = 0.0
    peak_pl: float = -1e18       # best executable P/L in price units since open
