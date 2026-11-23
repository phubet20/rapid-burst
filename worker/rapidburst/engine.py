"""Tick data layer (pure logic, no MetaTrader dependency).

Per symbol: a short raw tail of REAL ticks, the incremental TickScanner (scanners A/B/C, volatility unit,
typical spread) and the spread/quality gate. Decisions and exits live in ONE place: microburst.py.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional

from .config import EngineConfig
from .microburst import TickScanner


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
    """Light per-loop view (readiness, tick rate, volatility unit). All signal measurements live in
    microburst.TickScanner and are updated incrementally on every tick."""
    ready: bool = False
    reason: str = "warming up"
    t: float = 0.0
    mid: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    spread: float = 0.0
    vol_unit: float = 0.0
    tick_rate: float = 0.0
    score: float = 0.0           # filled with the strength for display

    def brief(self) -> dict:
        return {"ready": self.ready, "reason": self.reason, "score": round(self.score, 3), "spread": self.spread,
                "vu": self.vol_unit, "tps": round(self.tick_rate, 3)}


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


class MomentumEngine:
    """Per-symbol tick store + base features + spread gate (decisions live in microburst.py)."""

    def __init__(self, symbol: str, point: float, cfg: EngineConfig):
        self.symbol = symbol
        self.point = point if point > 0 else 1e-5
        self.cfg = cfg
        self.buf = TickBuffer(horizon=30.0)        # short raw tail (price sync / server clock only)
        self.scanner = TickScanner(cfg)
        self.last: Features = Features()

    # ------------------------------------------------------------------ ticks
    def on_tick(self, t: float, bid: float, ask: float) -> bool:
        ok = self.buf.add(Tick(t, bid, ask))
        if ok:
            self.scanner.push(t, bid, ask)          # scanners A/B/C updated incrementally, O(1) per tick
        return ok

    @property
    def seed_vu(self) -> float:
        return self.scanner.seed_vu

    @seed_vu.setter
    def seed_vu(self, v: float) -> None:
        self.scanner.seed_vu = v

    # --------------------------------------------------------------- features
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
        span = last.t - self.scanner.first_t
        if self.scanner.n < cfg.min_ticks_warmup or span < cfg.warmup_seconds:
            f.reason = (f"warming up: {self.scanner.n}/{cfg.min_ticks_warmup} ticks, "
                        f"{span:.0f}/{cfg.warmup_seconds:.0f}s of live data")
            self.last = f
            return f
        if now - last.t > cfg.max_tick_age:
            f.reason = f"market idle (last tick {now - last.t:.0f}s ago)"
            self.last = f
            return f
        f.tick_rate = self.scanner.tick_rate(last.t)
        f.vol_unit = self.scanner.vol_unit(self.point)
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
        typ = self.scanner.spread_ema
        if self.scanner.n > 30 and typ > 0 and f.spread > cfg.spread_spike_ratio * typ:
            return False, f"spread spike ({f.spread / typ:.1f}x typical)"
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
