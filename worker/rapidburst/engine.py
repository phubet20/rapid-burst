"""Tick-based momentum scalping engine (pure logic, no MetaTrader dependency).

The engine reads raw tick flow and computes, normalised by a short-term
volatility unit:
  * price displacement (fast / slow), velocity and acceleration
  * buying / selling pressure (tick-direction and magnitude imbalance)
  * micro-breakouts of short-term structure
  * compression -> expansion transitions and dead-market detection
  * exhaustion (price stretched from its 60s mean, decelerating)
  * pullback-and-renewal inside a short trend
  * spread / volatility / tick-rate quality

It blends these into one signed score in [-1, 1] instead of a rigid chain of
confirmations, so an entry can fire on the tick where the burst starts.
No RSI and no lagging oscillators are used.
"""
from __future__ import annotations

import math
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
    zscore: float = 0.0
    renewal: int = 0
    extended: bool = False
    score: float = 0.0

    def brief(self) -> dict:
        r = lambda x: round(x, 3)
        return {
            "ready": self.ready, "reason": self.reason, "score": r(self.score),
            "vel": r(self.d_fast), "acc": r(self.accel), "slow": r(self.d_slow),
            "press": r(self.pressure), "brk": self.breakout, "comp": self.compressed,
            "exp": r(self.expansion), "z": r(self.zscore), "renew": self.renewal,
            "ext": self.extended, "spread": self.spread, "vu": self.vol_unit,
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
    """Per-symbol engine. Feed ticks, ask for a signal or an exit decision."""

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

        # exhaustion: z-score of price vs 60s mean
        w60 = [x.mid for x in self.buf.window(last.t - 60.0, last.t)]
        if len(w60) > 5:
            mu = sum(w60) / len(w60)
            sd = math.sqrt(sum((x - mu) ** 2 for x in w60) / len(w60))
            f.zscore = (last.mid - mu) / sd if sd > 0 else 0.0

        # pullback-and-renewal inside a 30s trend
        p30 = self.buf.at(last.t - 30.0)
        if p30 is not None:
            trend = (last.mid - p30.mid) / vu
            if abs(trend) >= 1.5:
                s = 1 if trend > 0 else -1
                w15 = [x.mid for x in self.buf.window(last.t - 15.0, last.t - 1.0)]
                if w15:
                    extreme = max(w15) if s > 0 else min(w15)
                    trough = min(w15) if s > 0 else max(w15)
                    # retracement happened after the extreme and price is now pushing again
                    retr = (extreme - trough) * s / vu
                    if retr >= 0.8 and s * f.d_fast >= 0.7 and s * f.accel > 0:
                        f.renewal = s

        # extension / chase guard
        sd = 1 if f.d_slow > 0 else -1
        f.extended = (abs(f.zscore) >= cfg.exhaustion_z and (1 if f.zscore > 0 else -1) * f.accel < 0) or (
            abs(f.d_slow) >= cfg.chase_disp_units and sd * f.accel < 0.3
        )

        # blended score (impulse-led; other components add or subtract conviction)
        raw = (
            0.30 * math.tanh(f.d_fast / 2.5)
            + 0.15 * math.tanh(f.accel / 2.5)
            + 0.20 * f.pressure
            + 0.15 * f.breakout * (1.3 if f.compressed else 1.0)
            + 0.10 * f.renewal
            + 0.10 * math.tanh(f.d_slow / 4.0)
        )
        impulse_dir = 1 if f.d_fast > 0 else -1
        if abs(f.d_fast) < cfg.min_impulse_units or impulse_dir * f.pressure < cfg.min_pressure:
            raw *= 0.45       # no real one-sided displacement burst behind the reading
        if f.expansion < 0.6:
            raw *= 0.5        # dead / non-expanding market
        if f.extended and (raw > 0) == (f.d_slow > 0):
            raw *= 0.3        # do not chase stretched moves
        f.score = max(-1.0, min(1.0, raw))
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

    def signal(self, now: Optional[float] = None) -> Signal:
        f = self.features(now)
        cfg = self.cfg
        if not f.ready:
            return Signal("HOLD", 0.0, f.reason, f)
        if f.tick_rate < cfg.min_tick_rate:
            return Signal("HOLD", f.score, f"thin tick flow {f.tick_rate:.2f}/s", f)
        ok, why = self.spread_ok(f)
        if not ok:
            return Signal("HOLD", f.score, why, f)
        if abs(f.score) < cfg.entry_threshold:
            return Signal("HOLD", f.score, "no momentum edge", f)
        direction = 1 if f.score > 0 else -1
        if f.extended and direction * f.d_slow > 0:
            return Signal("HOLD", f.score, "move extended - not chasing", f)
        tags = []
        if f.breakout == direction:
            tags.append("breakout")
        if f.compressed and f.expansion > 1.2:
            tags.append("compression->expansion")
        if f.renewal == direction:
            tags.append("pullback-renewal")
        if direction * f.pressure > 0.5:
            tags.append("pressure")
        if direction * f.accel > 0.5:
            tags.append("accel")
        return Signal("BUY" if direction > 0 else "SELL", f.score, "+".join(tags) or "momentum", f)

    def exit_decision(self, pos: "PositionState", f: Features, now: float) -> Optional[str]:
        """Return a close reason or None. Uses executable price (bid for BUY, ask for SELL)."""
        cfg = self.cfg
        if not f.ready:
            return None
        d = 1 if pos.side == "BUY" else -1
        px = f.bid if d > 0 else f.ask
        vu = pos.entry_vu or f.vol_unit or self.point
        profit_u = (px - pos.open_price) * d / vu
        pos.peak_units = max(pos.peak_units, profit_u)
        held = now - pos.open_time
        if pos.peak_units >= cfg.profit_trigger_units and profit_u <= pos.peak_units * (1 - cfg.giveback_frac):
            return f"profit lock (peak {pos.peak_units:.1f}u -> {profit_u:.1f}u)"
        if profit_u >= cfg.weak_momentum_close_units and (
            d * f.d_fast < -0.5 or (d * f.pressure < -0.4 and d * f.accel < 0)
        ):
            return f"momentum weakening (+{profit_u:.1f}u)"
        if profit_u > 0.5 and d * f.zscore >= cfg.exhaustion_z and d * f.accel < 0:
            return f"exhaustion take-profit (+{profit_u:.1f}u)"
        if d * f.score <= -cfg.reversal_score:
            return f"reversal (score {f.score:+.2f})"
        # the spread is already inside profit_u (executable price): only react to loss beyond it
        spread_u = (f.spread / vu) if vu else 0.0
        if profit_u <= -(cfg.adverse_fade_units + spread_u) and d * f.d_fast < 0 and d * f.pressure < 0:
            return f"adverse fade ({profit_u:+.1f}u, momentum against)"
        if held >= cfg.max_hold_seconds:
            return f"time stop {held:.0f}s ({profit_u:+.1f}u)"
        if held >= cfg.max_hold_seconds / 2 and profit_u <= -cfg.stale_loss_units:
            return f"setup invalid ({profit_u:+.1f}u)"
        return None


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
