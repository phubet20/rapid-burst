"""M1 Rapid Basket Scalper (t30u, M1 only): scanners, decision and the basket exit (pure logic).

ONE engine. Five families are scored on the same moment and feed ONE decision:
  A. tick velocity & acceleration  - tick rate, velocity, acceleration/deceleration, direction sequence,
                                     directional efficiency, early acceleration, exhaustion
  B. activity burst & candle pressure - relative tick activity, activity expansion, intrabar body expansion,
                                     body/range, wick rejection, consecutive pressure, activity vs price
                                     response, absorption
  C. micro-structure trigger        - micro high/low, compression -> expansion, micro breakout, failed
                                     breakout, structure break, pullback recovery, short rebound, anti-chase
  D. M1 entry timing (mtf.py)       - forming-bar direction/body expansion, MACD histogram expansion and slope
                                     change (no full cross needed), RSI level/slope (soft), tick-volume expansion,\n                                     continuation with M5
  E. M1 main trend (mtf.py; t32u: the dead M5 timeframe is removed)     - EMA trend, body/range expansion, MACD direction/histogram/momentum change, RSI,
                                     tick-volume activity, HH/HL vs LH/LL, supported vs exhausting
The decision scores how well they AGREE; no single family is mandatory, activity alone never trades, M5 is a
filter (soft penalty when ambiguous/weak against, veto only when clearly against and supported).

Speed: tick measurements are kept INCREMENTALLY by TickScanner.push() (rolling windows with running sums and
monotonic min/max deques, O(1) amortised per tick); EMA/MACD advance once per closed bar and the forming bar
is an O(1) peek. Reading a snapshot never rescans history.

Honesty notes (also shown in the UI):
  * "tick volume"/"activity" = number of price updates the broker counted/sent. It is NOT exchange volume.
  * "flow"/"imbalance" = up- vs down-ticks and bid/ask changes: a PROXY for pressure, not real order flow.
  * Market depth is not used: neither TickerAll nor this MT5 bridge exposes a reliable book.
RSI(14) is a SOFT feature inside D (level, slope, momentum change) - never a gate, never a cross wait.\nNo fixed confirmation steps, no waiting for the candle close.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from statistics import median
from typing import Optional

from .mtf import ip_eval, m1_sniper


def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def _sgn(x: float) -> int:
    return 1 if x > 0 else -1 if x < 0 else 0


@dataclass
class Micro:
    """Everything the engine knows about the last few seconds of one symbol."""
    ready: bool = False
    reason: str = "warming up"
    t: float = 0.0
    mid: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    spread: float = 0.0
    atr: float = 0.0
    vu: float = 0.0                 # typical 3 s move (price units)
    # A. tick flow & micro momentum
    tick_rate: float = 0.0          # ticks/s over the last 5 s
    gap_lim: float = 0.0            # per-symbol allowed tick gap (t32u)
    rate_ratio: float = 1.0         # vs the 60 s baseline
    seq_run: int = 0                # signed length of the current same-direction tick run
    seq_consist: float = 0.0        # mean direction of the last 12 price changes (-1..1)
    vel: float = 0.0                # move over the velocity window, in ATR
    vel_prev: float = 0.0
    accel: float = 0.0              # vel - vel_prev (ATR)
    eff: float = 0.0                # directional efficiency over 5 s (-1..1)
    burst_z: float = 0.0            # |3 s move| / typical 3 s move
    burst_dir: int = 0
    # B. tick volume (activity count, NOT real volume)
    act_ratio: float = 1.0
    absorption: bool = False        # activity up, price not moving
    # C. live candle
    bar_age: float = 0.0
    body: float = 0.0               # close - open (ATR)
    body_range: float = 0.0
    upper_wick: float = 0.0         # fraction of range
    lower_wick: float = 0.0
    close_loc: float = 0.5
    range_atr: float = 0.0
    range_exp: float = 0.0          # live range / average previous range
    consec: int = 0                 # signed count of consecutive same-direction closed M1 candles
    abnormal: bool = False
    rejection: int = 0              # +1 bullish (long lower wick), -1 bearish (long upper wick)
    # D. micro structure
    micro_hi: float = 0.0
    micro_lo: float = 0.0
    breakout: int = 0
    failed: int = 0                 # +1 = failed breakdown (bullish), -1 = failed breakout (bearish)
    struct_break: int = 0
    compressed: bool = False
    expansion: float = 0.0
    # E. pullback & continuation, anti-chase
    pb_dir: int = 0
    pb_depth: float = 0.0
    continuation: int = 0
    weak_return: bool = False
    chase_up: float = 0.0           # ATR above the recent swing low
    chase_dn: float = 0.0           # ATR below the recent swing high
    origin_up: float = 0.0          # time of that swing low  (burst identity for duplicate prevention)
    origin_dn: float = 0.0
    # F. pressure proxy
    imbalance: float = 0.0          # up vs down ticks (proxy)
    bidask: float = 0.0             # bid/ask up vs down changes (proxy)
    depth: str = "n/a (no reliable market depth from this broker)"
    # G. volatility & execution context
    spread_ratio: float = 1.0
    target: float = 0.0             # adaptive profit capture distance (price)
    spread_target: float = 0.0
    slip_risk: float = 0.0          # 0..1
    latency: float = 0.0            # last tick age (s)
    gap_max: float = 0.0            # largest gap between ticks in the last 10 s
    stable: bool = True
    regime: str = "normal"          # fast / normal / quiet / chop
    # pre-burst extras
    rebound: int = 0                # +1 short rebound off the micro low, -1 off the micro high
    exhaustion: int = 0             # +1 an up-move is exhausting (extended + decelerating), -1 a down-move
    early: int = 0                  # +1/-1 early acceleration out of compression (pre-burst)
    resp: float = 0.0               # price response per unit of activity (absorption check)
    n_ticks: int = 0
    # multi-timeframe (real broker M1/M5 candles + forming bar from ticks), set by the trader
    mtf: Optional[object] = None    # mtf.MTFView
    ind: Optional[object] = None    # indicators.IndView (t32u confluence layer, family F)
    ind_w: Optional[dict] = None    # learned indicator weights for this symbol
    zones: Optional[object] = None  # zones.ZoneView (t37u: DM/SP zones, FVG, confirmation candle, wick tip)
    trend: Optional[object] = None  # regime.RegimeView (t40u: UP / DOWN / SIDEWAYS + phase)
    prec: Optional[object] = None   # precision.PrecisionView (t45u: M5 structure, OB, liquidity, displacement)

    def brief(self) -> dict:
        r = lambda x, n=3: round(x, n)   # noqa: E731
        return {"regime": self.regime, "tps": r(self.tick_rate, 2), "rate_x": r(self.rate_ratio, 2),
                "vel_atr": r(self.vel), "acc_atr": r(self.accel), "eff": r(self.eff, 2), "burst_z": r(self.burst_z, 2),
                "act_x": r(self.act_ratio, 2), "absorb": self.absorption, "imb": r(self.imbalance, 2),
                "bidask": r(self.bidask, 2), "seq": self.seq_run, "body_atr": r(self.body), "body_rng": r(self.body_range, 2),
                "close_loc": r(self.close_loc, 2), "range_x": r(self.range_exp, 2), "consec": self.consec,
                "reject": self.rejection, "brk": self.breakout, "failed": self.failed, "struct": self.struct_break,
                "comp": self.compressed, "exp": r(self.expansion, 2), "pb": r(self.pb_depth, 2), "cont": self.continuation,
                "chase_up": r(self.chase_up, 2), "chase_dn": r(self.chase_dn, 2), "spread_x": r(self.spread_ratio, 2),
                "spread_tgt": r(self.spread_target, 2), "slip_risk": r(self.slip_risk, 2), "lat": r(self.latency, 1),
                "gap": r(self.gap_max, 1), "target": self.target, "atr": self.atr, "spread": self.spread,
                "rebound": self.rebound, "exhaust": self.exhaustion, "early": self.early}


# =============================================================== incremental building blocks
class _Roll:
    """Rolling window [now - span, now] of tick changes with running sums (O(1) amortised per tick).
    `edge` = mid of the last tick that left the window = the price in force at now - span."""
    __slots__ = ("span", "q", "n", "sd", "sa", "up", "dn", "ba", "bt", "edge", "_k")

    def __init__(self, span: float):
        self.span = span
        self.q: deque = deque()
        self.n = 0
        self.sd = self.sa = 0.0
        self.up = self.dn = self.ba = self.bt = 0
        self.edge: Optional[float] = None
        self._k = 0

    def push(self, t: float, mid: float, d: float, bas: int, bat: int) -> None:
        self.q.append((t, mid, d, bas, bat))
        self.n += 1
        self.sd += d
        self.sa += abs(d)
        self.up += d > 0
        self.dn += d < 0
        self.ba += bas
        self.bt += bat

    def expire(self, now: float) -> None:
        lim = now - self.span
        q = self.q
        while q and q[0][0] < lim:
            t, mid, d, bas, bat = q.popleft()
            self.edge = mid
            self.n -= 1
            self.sd -= d
            self.sa -= abs(d)
            self.up -= d > 0
            self.dn -= d < 0
            self.ba -= bas
            self.bt -= bat
            self._k += 1
        if self._k >= 2000:                      # bound float drift of the running sums
            self._k = 0
            self.sd = sum(x[2] for x in q)
            self.sa = sum(abs(x[2]) for x in q)


class _Ext:
    """Rolling max/min of mid over [now - span, now - lag] with the time of each extreme (monotonic deques)."""
    __slots__ = ("span", "lag", "pend", "mx", "mn")

    def __init__(self, span: float, lag: float = 0.0):
        self.span, self.lag = span, lag
        self.pend: deque = deque()
        self.mx: deque = deque()
        self.mn: deque = deque()

    def _add(self, t: float, v: float) -> None:
        mx, mn = self.mx, self.mn
        while mx and mx[-1][1] <= v:
            mx.pop()
        mx.append((t, v))
        while mn and mn[-1][1] >= v:
            mn.pop()
        mn.append((t, v))

    def push(self, t: float, v: float) -> None:
        if self.lag > 0:
            self.pend.append((t, v))
        else:
            self._add(t, v)

    def update(self, now: float) -> None:
        cut = now - self.lag
        pend = self.pend
        while pend and pend[0][0] <= cut:
            self._add(*pend.popleft())
        lim = now - self.span
        while self.mx and self.mx[0][0] < lim:
            self.mx.popleft()
        while self.mn and self.mn[0][0] < lim:
            self.mn.popleft()

    @property
    def ok(self) -> bool:
        return bool(self.mx)

    @property
    def hi(self) -> float:
        return self.mx[0][1]

    @property
    def lo(self) -> float:
        return self.mn[0][1]

    @property
    def hi_t(self) -> float:
        return self.mx[0][0]

    @property
    def lo_t(self) -> float:
        return self.mn[0][0]

    def min_after(self, t0: float) -> Optional[float]:
        for t, v in self.mn:                     # mn is increasing in time AND value: first t >= t0 is the min
            if t >= t0:
                return v
        return None

    def max_after(self, t0: float) -> Optional[float]:
        for t, v in self.mx:
            if t >= t0:
                return v
        return None


# =================================================================== the scanners (one stream)
class TickScanner:
    """Scanners A, B and C fed by every real tick (push) and read by snapshot(). One instance per symbol."""

    def __init__(self, cfg):
        self.cfg = cfg
        W = cfg.mb_vel_seconds
        self.n = 0
        self.first_t = 0.0
        self.last: Optional[tuple] = None          # (t, bid, ask, mid)
        self.r_vel = _Roll(W)                      # A: velocity window
        self.r_vel2 = _Roll(2 * W)                 # A: previous velocity window edge
        self.r_flow = _Roll(cfg.mb_flow_seconds)   # B: tick-direction / bid-ask proxies
        self.r3 = _Roll(3.0)
        self.r5 = _Roll(5.0)                       # A: tick rate, efficiency
        self.r20 = _Roll(20.0)
        self.r30 = _Roll(30.0)
        self.r60 = _Roll(60.0)                     # B: activity baseline
        self.x_struct = _Ext(cfg.mb_struct_seconds, 3.0)   # C: micro high/low (excl. the last 3 s)
        self.x10 = _Ext(10.0)                      # C: recent excursion (failed breakout)
        self.x10l = _Ext(10.0, 2.0)                # C: structure break reference
        self.x_comp = _Ext(15.0, 3.0)              # C: compression
        self.x3 = _Ext(3.0)                        # C: expansion
        self.x5 = _Ext(5.0)                        # C: short rebound
        self.x_swing = _Ext(cfg.mb_swing_seconds)  # anti-chase + burst identity
        self.x30 = _Ext(30.0)                      # pullback & chop
        self.x60 = _Ext(60.0)                      # volatility estimate without broker bars
        self.seq_run = 0
        self.signs: deque = deque(maxlen=12)
        self.sign_sum = 0
        self.blk_id = None                         # 5 s block ranges -> typical range (compression)
        self.blk_hi = self.blk_lo = 0.0
        self.blocks: deque = deque(maxlen=24)
        self.r5_typ = 0.0
        self.bar_min = None                        # live M1 candle, built from ticks
        self.bar_o = self.bar_h = self.bar_l = 0.0
        self.gaps: deque = deque()                 # (t, gap) monotonic max over 10 s
        self.spread_ema = 0.0
        self.grid_t = None                         # 1 s grid for the volatility unit
        self.grid: deque = deque(maxlen=int(cfg.vol_window) + 1)
        self.vu_k = max(1, int(round(cfg.fast_window)))
        self.vu_diffs: deque = deque()
        self.vu_sum = 0.0
        self.seed_vu = 0.0

    # ------------------------------------------------------------------ ingest
    def push(self, t: float, bid: float, ask: float) -> None:
        mid = (bid + ask) / 2.0
        if self.last is None:
            self.first_t = t
            d, bas, bat = 0.0, 0, 0
            self.spread_ema = ask - bid
            self.grid_t = math.floor(t)
        else:
            pt, pb, pa, pm = self.last
            self._grid(t, pm)
            d = mid - pm
            bas = (1 if bid > pb else -1 if bid < pb else 0) + (1 if ask > pa else -1 if ask < pa else 0)
            bat = (bid != pb) + (ask != pa)
            g = t - pt
            while self.gaps and self.gaps[-1][1] <= g:
                self.gaps.pop()
            self.gaps.append((t, g))
            self.spread_ema += (ask - bid - self.spread_ema) / 150.0
            if d != 0:
                s = 1 if d > 0 else -1
                self.seq_run = self.seq_run + s if (self.seq_run == 0 or (self.seq_run > 0) == (s > 0)) else s
                if len(self.signs) == self.signs.maxlen:
                    self.sign_sum -= self.signs[0]
                self.signs.append(s)
                self.sign_sum += s
        self.n += 1
        prev_mid = self.last[3] if self.last else mid
        self.last = (t, bid, ask, mid)
        for r in (self.r_vel, self.r_vel2, self.r_flow, self.r3, self.r5, self.r20, self.r30, self.r60):
            r.push(t, mid, d, bas, bat)
            r.expire(t)
        for x in (self.x_struct, self.x10, self.x10l, self.x_comp, self.x3, self.x5, self.x_swing, self.x30, self.x60):
            x.push(t, mid)
            x.update(t)
        while self.gaps and self.gaps[0][0] < t - 10.0:
            self.gaps.popleft()
        b = math.floor(t / 5.0)
        if b != self.blk_id:
            if self.blk_id is not None and self.blk_hi > self.blk_lo:
                self.blocks.append(self.blk_hi - self.blk_lo)
                self.r5_typ = median(self.blocks)          # <= 24 values, once per 5 s
            self.blk_id, self.blk_hi, self.blk_lo = b, mid, mid
        else:
            self.blk_hi, self.blk_lo = max(self.blk_hi, mid), min(self.blk_lo, mid)
        mnt = math.floor(t / 60.0)
        if mnt != self.bar_min:
            self.bar_min = mnt
            self.bar_o = prev_mid
            self.bar_h, self.bar_l = max(prev_mid, mid), min(prev_mid, mid)
        else:
            self.bar_h, self.bar_l = max(self.bar_h, mid), min(self.bar_l, mid)

    def _grid(self, t: float, mid_before: float) -> None:
        """Sample the mid on a 1 s grid (price in force at each whole second) for the volatility unit."""
        k = self.vu_k
        maxd = max(1, int(self.cfg.vol_window) - k)
        while self.grid_t + 1.0 <= t:
            self.grid_t += 1.0
            self.grid.append(mid_before)
            if len(self.grid) > k:
                dv = abs(self.grid[-1] - self.grid[-1 - k])
                self.vu_diffs.append(dv)
                self.vu_sum += dv
                if len(self.vu_diffs) > maxd:
                    self.vu_sum -= self.vu_diffs.popleft()
            if t - self.grid_t > self.cfg.vol_window:      # long gap: jump ahead
                self.grid_t = math.floor(t) - self.cfg.vol_window

    # ------------------------------------------------------------------ reads
    def vol_unit(self, point: float) -> float:
        vu = self.vu_sum / len(self.vu_diffs) if self.vu_diffs else 0.0
        if self.seed_vu > 0 and self.last:
            w = _clip((self.last[0] - self.first_t) / 30.0)
            vu = w * vu + (1.0 - w) * self.seed_vu if self.vu_diffs else self.seed_vu
        return max(vu, 2.0 * point)

    def tick_rate(self, now: float) -> float:
        self.r30.expire(now)
        return self.r30.n / max(2.0, min(30.0, now - self.first_t))

    def atr_estimate(self, now: float) -> float:
        """M1-range estimate from live ticks when the broker gives no M1 history (needs >= 60 s of ticks)."""
        if not self.last or self.last[0] - self.first_t < 60.0:
            return 0.0
        self.x60.update(now)
        return (self.x60.hi - self.x60.lo) if self.x60.ok else 0.0

    def snapshot(self, now: float, atr: float, vu: float, bars: list, cfg, slip_hist=None) -> Micro:
        """Everything the decision needs, read from the incremental state (no history rescans)."""
        m = Micro()
        if self.last is None:
            m.reason = "no live ticks"
            return m
        t_last, bid, ask, m0 = self.last
        m.t, m.mid, m.bid, m.ask, m.spread = t_last, m0, bid, ask, ask - bid
        m.n_ticks = self.n
        m.latency = max(0.0, now - t_last)
        span = t_last - self.first_t
        if self.n < cfg.min_ticks_warmup or span < cfg.warmup_seconds:
            m.reason = f"warming up: {self.n}/{cfg.min_ticks_warmup} ticks, {span:.1f}/{cfg.warmup_seconds:.1f}s"
            return m
        if m.latency > cfg.max_tick_age:
            m.reason = f"market idle (last tick {m.latency:.0f}s ago)"
            return m
        vu = max(vu, 1e-12)
        atr = atr if atr > 0 else 4.0 * vu
        m.atr, m.vu = atr, vu
        now = t_last if now - t_last < 1.0 else now
        for r in (self.r_vel, self.r_vel2, self.r_flow, self.r3, self.r5, self.r20, self.r30, self.r60):
            r.expire(now)
        for x in (self.x_struct, self.x10, self.x10l, self.x_comp, self.x3, self.x5, self.x_swing, self.x30):
            x.update(now)
        first = self.r60.q[0][1] if self.r60.q else m0
        edge = lambda r: r.edge if r.edge is not None else first   # noqa: E731

        # ---- A. tick velocity & acceleration
        m.tick_rate = self.r5.n / 5.0
        base_rate = self.r60.n / max(5.0, min(60.0, now - self.first_t))
        m.rate_ratio = m.tick_rate / base_rate if base_rate > 0 else 1.0
        m1, m2 = edge(self.r_vel), edge(self.r_vel2)
        m.vel, m.vel_prev = (m0 - m1) / atr, (m1 - m2) / atr
        m.accel = m.vel - m.vel_prev
        m.eff = (self.r5.sd / self.r5.sa) if self.r5.sa > 0 else 0.0
        m3 = m0 - edge(self.r3)
        m.burst_z, m.burst_dir = abs(m3) / vu, _sgn(m3)
        if self.signs:
            m.seq_consist = self.sign_sum / len(self.signs)
        m.seq_run = self.seq_run

        # ---- B. activity (count of quotes, NOT volume) & live candle pressure
        m.act_ratio = m.rate_ratio
        m5 = abs(m0 - edge(self.r5))
        m.resp = m5 / vu
        m.absorption = m.act_ratio >= cfg.mb_absorb_activity and m5 < 0.5 * vu and abs(m.eff) < 0.3
        bs = math.floor(now / 60.0) * 60.0
        if self.bar_min == math.floor(now / 60.0):
            o, hi, lo = self.bar_o, self.bar_h, self.bar_l
        else:
            o = hi = lo = m0
        c = m0
        rng = hi - lo
        m.bar_age = now - bs
        m.body = (c - o) / atr
        m.range_atr = rng / atr
        if rng > 0:
            m.body_range = abs(c - o) / rng
            m.upper_wick = (hi - max(o, c)) / rng
            m.lower_wick = (min(o, c) - lo) / rng
            m.close_loc = (c - lo) / rng
        prev = [b for b in (bars or [])[-8:] if b.get("high") is not None and b.get("time", 0) < bs][-6:]
        pr = [b["high"] - b["low"] for b in prev if b["high"] > b["low"]]
        avg_r = sum(pr) / len(pr) if pr else atr
        m.range_exp = rng / avg_r if avg_r > 0 else 0.0
        cons = 0
        for b in reversed(prev):
            d = _sgn(b["close"] - b["open"])
            if d == 0 or (cons and _sgn(cons) != d):
                break
            cons += d
        m.consec = cons
        m.abnormal = m.range_atr >= cfg.mb_abnormal_range_atr and m.bar_age <= 40.0
        if rng >= 0.25 * atr:
            if m.upper_wick >= cfg.mb_wick_reject:
                m.rejection = -1
            elif m.lower_wick >= cfg.mb_wick_reject:
                m.rejection = 1
        m.imbalance = (self.r_flow.up - self.r_flow.dn) / (self.r_flow.up + self.r_flow.dn) \
            if (self.r_flow.up + self.r_flow.dn) else 0.0
        m.bidask = self.r_flow.ba / self.r_flow.bt if self.r_flow.bt else 0.0

        # ---- C. micro structure & pre-burst trigger
        buf = max(0.1 * vu, 0.5 * m.spread)
        xs = self.x_struct
        if xs.ok:
            m.micro_hi, m.micro_lo = xs.hi, xs.lo
            if m0 > m.micro_hi + buf:
                m.breakout = 1
            elif m0 < m.micro_lo - buf:
                m.breakout = -1
            if self.x10.ok:
                if self.x10.hi > m.micro_hi + buf and m0 < m.micro_hi - buf:
                    m.failed = -1
                elif self.x10.lo < m.micro_lo - buf and m0 > m.micro_lo + buf:
                    m.failed = 1
        d20 = m0 - edge(self.r20)
        if self.x10l.ok and abs(d20) >= vu:
            if d20 > 0 and m0 < self.x10l.lo - buf:
                m.struct_break = -1
            elif d20 < 0 and m0 > self.x10l.hi + buf:
                m.struct_break = 1
        r5 = max(self.r5_typ or vu, 1e-12)
        if self.x_comp.ok:
            m.compressed = (self.x_comp.hi - self.x_comp.lo) < cfg.mb_compress_ratio * r5
        e3 = edge(self.r3)
        if self.x3.ok:
            m.expansion = (max(self.x3.hi, e3) - min(self.x3.lo, e3)) / r5
        if self.x5.ok and xs.ok:
            if self.x5.lo <= m.micro_lo + buf and m0 - self.x5.lo >= 0.3 * vu and m.vel > 0:
                m.rebound = 1
            elif self.x5.hi >= m.micro_hi - buf and self.x5.hi - m0 >= 0.3 * vu and m.vel < 0:
                m.rebound = -1
        # pullback & continuation (recovery after a pullback inside a 30 s move)
        p30 = edge(self.r30)
        tr = m0 - p30
        if abs(tr) >= 1.5 * vu and self.x30.ok:
            s = _sgn(tr)
            if s > 0:
                ext, trough = self.x30.hi, self.x30.min_after(self.x30.hi_t)
            else:
                ext, trough = self.x30.lo, self.x30.max_after(self.x30.lo_t)
            trough = m0 if trough is None else trough
            leg = (ext - p30) * s
            m.pb_dir = s
            m.pb_depth = ((ext - trough) * s / leg) if leg > 0 else 0.0
            off = (m0 - trough) * s
            if 0.15 <= m.pb_depth <= 0.7 and s * m.vel > 0 and s * m.accel >= 0 and off >= 0.3 * vu:
                m.continuation = s
            elif m.pb_depth > 0.15 and off < 0.3 * (ext - trough) * s and s * m.vel <= 0:
                m.weak_return = True
        sw = self.x_swing
        if sw.ok:
            m.chase_up, m.origin_up = (m0 - sw.lo) / atr, sw.lo_t
            m.chase_dn, m.origin_dn = (sw.hi - m0) / atr, sw.hi_t
        # early acceleration out of compression / exhaustion of an extended move
        if m.compressed or m.expansion < 1.5:
            if m.vel > 0 and m.accel > 0 and m.chase_up <= 0.45:
                m.early = 1
            elif m.vel < 0 and m.accel < 0 and m.chase_dn <= 0.45:
                m.early = -1
        if m.chase_up >= 0.6 * cfg.mb_max_chase_atr and m.vel_prev > 0 and m.vel < 0.5 * m.vel_prev:
            m.exhaustion = 1
        elif m.chase_dn >= 0.6 * cfg.mb_max_chase_atr and m.vel_prev < 0 and m.vel > 0.5 * m.vel_prev:
            m.exhaustion = -1

        # ---- context: spread, feed, slippage, regime, target
        typ = self.spread_ema
        m.spread_ratio = m.spread / typ if typ > 0 else 1.0
        while self.gaps and self.gaps[0][0] < now - 10.0:
            self.gaps.popleft()
        m.gap_max = max(self.gaps[0][1] if self.gaps else 0.0, m.latency)
        # t32u (15): per-symbol feed check - a quiet FX pair ticks less often than BTC; the gap limit adapts to
        # the symbol's own normal tick spacing (4x, capped at 20 s); latency still uses the configured cap
        typ_gap = 30.0 / max(self.r30.n, 1.5)
        m.gap_lim = min(20.0, max(cfg.mb_max_gap_s, 4.0 * typ_gap))
        m.stable = m.gap_max <= m.gap_lim and m.latency <= cfg.mb_max_gap_s
        fast = m.rate_ratio >= 1.6 and (m.burst_z >= 2.0 or m.range_exp >= 1.2)
        if fast:
            m.regime = "fast"
        elif m.tick_rate < cfg.min_tick_rate or vu < 0.03 * atr:
            m.regime = "quiet"
        elif abs(m.eff) < 0.25 and (self.x30.hi - self.x30.lo) > 0.8 * atr and abs(tr) < 0.3 * atr:
            m.regime = "chop"
        else:
            m.regime = "normal"
        mult = {"fast": 1.3, "normal": 1.0, "quiet": 0.8, "chop": 0.8}[m.regime]
        m.target = max(cfg.mb_target_atr * atr * mult, cfg.mb_target_spread * m.spread)
        m.spread_target = m.spread / m.target if m.target > 0 else 1.0
        sl_avg = (sum(slip_hist) / len(slip_hist)) if slip_hist else 0.0
        m.slip_risk = _clip(0.4 * _clip((m.spread_ratio - 1.0) / 1.5) + 0.3 * _clip(m.gap_max / cfg.mb_max_gap_s)
                            + 0.3 * _clip(sl_avg / max(cfg.max_avg_slippage_vu, 1e-9)))
        m.ready, m.reason = True, "ok"
        return m


# ======================================================================= ONE decision
@dataclass
class Strength:
    buy: float = 0.0
    sell: float = 0.0
    threshold: float = 1.0
    side: str = "NO TRADE"
    reason: str = ""
    origin: float = 0.0
    comps: dict = field(default_factory=dict)
    scan: dict = field(default_factory=dict)        # {"BUY": {"A":..,"B":..,"C":..}, "SELL": {...}}
    support: dict = field(default_factory=dict)
    vetoes: list = field(default_factory=list)
    pe: dict = field(default_factory=dict)         # t45u: Position Entry Score per side (separate from buy/sell momentum)
    plans: dict = field(default_factory=dict)      # t45u: precision.Plan per side (structural stop, Target LIQ, R:R)
    bar_side: str = ""                             # t46u: M1 trend side for the every-M1-bar entry ("" = not available)
    bar_reason: str = ""
    ip_key: tuple = ()                             # t48u: (leg start time, leg end time) of the impulse -> one entry per pullback
    ip_stop: float = 0.0                           # t48u: the pullback extreme (the structural stop goes beyond it)
    ip_info: dict = field(default_factory=dict)

    @property
    def score(self) -> float:
        return self.buy if self.side == "BUY" else -self.sell if self.side == "SELL" else (self.buy - self.sell)


SCAN_W = {"fast": (1.3, 0.9, 0.8), "normal": (1.0, 1.0, 1.0), "quiet": (0.8, 0.8, 1.3), "chop": (0.8, 1.0, 1.2)}
W_M1, W_TREND, W_IND = 1.0, 0.8, 1.2   # D = M1 timing, E = M1 main trend, F = indicator confluence (t32u)
FAMILIES = ("A", "B", "C", "D", "E", "F")


def _mtf_scores(m: Micro, d: int, cfg=None) -> tuple:
    """(D, E, M1 trend bias signed for this side) or None for a family whose timeframe has no data yet."""
    v = m.mtf
    if v is None:
        return None, None, 0.0
    dd = v.timing(d) if v.m1.ready else None
    # t34u: E = M5 MAIN MOMENTUM on the forming M5 bar (no close wait); before M5 bars exist, the M1 trend
    if v.m5.ready and not (cfg is not None and getattr(cfg, "m1_only", False)):
        e = v.m5_momentum(d)
        return dd, e, d * v.m5_bias
    e = _clip(d * v.bias / 0.6) if v.m1.ready else None
    return dd, e, d * v.bias


def _components(m: Micro, d: int, cfg) -> dict:
    cdir = _sgn(m.body)
    body_exp = _clip(m.range_exp - 0.5) if cdir == d else 0.0
    candle = (0.35 * _clip(d * cdir * m.body_range) + 0.35 * _clip(d * (m.close_loc - 0.5) * 2)
              + 0.15 * body_exp + 0.15 * (1.0 if d * m.consec >= 2 else 0.0))
    struct = 0.0
    if m.breakout == d:
        struct = max(struct, 1.0 if m.compressed else 0.7)
    if m.failed == d:
        struct = max(struct, 0.8)
    if m.struct_break == d:
        struct = max(struct, 0.7)
    if m.continuation == d:
        struct = max(struct, 0.9)
    if m.rebound == d:
        struct = max(struct, 0.6)
    exp_start = _clip((m.expansion - 1.0) / 1.5) if m.burst_dir == d else 0.0
    responds = d * m.vel > 0
    return {"velocity": _clip(d * m.vel / cfg.mb_vel_ref_atr),
            "accel": _clip(d * m.accel / (0.6 * cfg.mb_vel_ref_atr)),
            "efficiency": _clip(d * m.eff),
            "sequence": 0.5 * _clip(d * m.seq_run / 5.0) + 0.5 * _clip(d * m.seq_consist),
            "burst": _clip((m.burst_z - 1.0) / 2.0) if m.burst_dir == d else 0.0,
            "activity": _clip(m.act_ratio - 1.0) if responds else 0.0,
            "flow": 0.5 * _clip(d * m.imbalance) + 0.5 * _clip(d * m.bidask),
            "candle": _clip(candle),
            "structure": _clip(struct + 0.3 * exp_start)}


def _scanners(c: dict) -> tuple[float, float, float]:
    a = (1.0 * c["velocity"] + 0.7 * c["accel"] + 0.8 * c["efficiency"] + 0.5 * c["sequence"] + 0.6 * c["burst"]) / 3.6
    b = (0.6 * c["activity"] + 0.9 * c["flow"] + 0.8 * c["candle"]) / 2.3
    return a, b, c["structure"]


def threshold(m: Micro, cfg) -> float:
    adj = {"fast": -0.05, "normal": 0.0, "quiet": 0.05, "chop": 0.08}[m.regime]
    return _clip(cfg.mb_threshold + adj + 0.12 * max(0.0, m.spread_ratio - 1.2) + 0.15 * m.slip_risk, 0.2, 0.95)


def side_strength(m: Micro, d: int, cfg) -> tuple[float, dict, tuple, list]:
    """One strength per side from five families scored on the same moment: A tick velocity/acceleration,
    B activity & candle pressure, C micro-structure, D M1 timing (MACD histogram expansion/turn, body, tick
    volume, continuation), E M5 direction bias. Families without data (no bars yet) drop out of the mean."""
    c = _components(m, d, cfg)
    a, b, cs = _scanners(c)
    dd, e, bias = _mtf_scores(m, d, cfg)
    wa, wb, wc = SCAN_W.get(m.regime, SCAN_W["normal"])
    num, den = wa * a + wb * b + wc * cs, wa + wb + wc
    if dd is not None:
        num, den = num + W_M1 * dd, den + W_M1
        c["m1_timing"] = dd
    if e is not None:
        num, den = num + W_TREND * e, den + W_TREND
        c["m5_momentum"] = e
    iv = m.ind
    f = None
    if iv is not None and getattr(iv, "ready", False) and getattr(cfg, "ind_enabled", True):
        f = iv.score(d, m.ind_w)
        num, den = num + W_IND * f, den + W_IND
        c["confluence"] = f
    s = num / den
    zv = m.zones
    if zv is not None and getattr(zv, "ready", False) and getattr(cfg, "zones_enabled", True):
        z = zv.score(d)                                # t37u: zones / FVG / confirmation / wick tip as a score input
        s += getattr(cfg, "zone_weight", 0.10) * (z - 0.5) * 2.0
        c["zones"] = z
    vetoes = []
    rv = m.trend
    if (rv is not None and getattr(rv, "ready", False) and getattr(cfg, "regime_enabled", True)
            and not getattr(cfg, "m1_only", False)):
        zv2 = m.zones if (m.zones is not None and getattr(m.zones, "ready", False)) else None
        wick = zv2 is not None and zv2.wick == d
        in_zone = zv2 is not None and zv2.holds(d)
        bonus = getattr(cfg, "regime_bonus", 0.10)
        if rv.dir == -d:
            s -= 0.15                                  # against the remembered state (a structure break flips it first)
            if getattr(cfg, "regime_counter_veto", True) and not (wick or in_zone):
                # t44u: no hedge safety net any more - do not trade against an INTACT M5 structure unless a wick /
                # zone backs it (a real reversal breaks the structure first, which flips the state: not blocked)
                vetoes.append(f"against the M5 structure ({rv.state} phase {rv.phase}) with no wick / zone to back it")
        elif rv.dir == d:
            ext = d * (m.mid - rv.origin) / max(rv.atr, 1e-12) if rv.origin else 0.0
            if ext > getattr(cfg, "regime_chase_atr", 2.0) and not wick and not in_zone:
                vetoes.append(f"already spiked {ext:.1f} ATR from the pullback origin - wait for the next pullback wick "
                              f"({rv.state} phase {rv.phase})")
            elif rv.leg == "pullback" or wick or in_zone:
                s += bonus                             # pullback wick / zone in the trend: enter at the wick tip
        else:
            rng = max(rv.range_hi - rv.range_lo, 1e-12)
            pos = (m.mid - rv.range_lo) / rng
            edge = pos <= 0.3 if d > 0 else pos >= 0.7
            if edge and (wick or in_zone):
                s += bonus                             # sideways: fade the range edge at a wick
            elif 0.35 < pos < 0.65:
                s -= getattr(cfg, "regime_mid_penalty", 0.06)   # middle of the range: weak (t44u: 0.06 -> 0.10)
        c["regime"] = rv.dir * d
    if f is not None:
        from .indicators import sr_block
        sr = sr_block(iv, d, m.mid, m.atr, cfg.ind_sr_near_atr)
        if sr:
            s -= 0.06                                  # t34u: a nearby level is a penalty, not a lock
        if iv.chop and cfg.ind_chop_veto:
            vetoes.append(f"chop: {iv.chop_why}")
        elif iv.chop:
            s -= 0.04
        if f < cfg.ind_min_score:
            vetoes.append(f"indicator confluence {f:.2f} < {cfg.ind_min_score:.2f} against the side")
    v = m.mtf
    if v is not None and v.m1.ready:
        # t34u: the higher-frame direction only WEIGHS (a reversal starts against it); the hard trend veto and the
        # swing-break veto blocked every reversal entry
        if bias < 0:
            s -= 0.08 * min(1.0, -bias / 0.6)
        elif bias >= 0.3:
            s += 0.05
        if v.m1.exhaust == d:
            s -= 0.06
        if v.m1.sbreak == -d:
            s -= 0.05
    if m.early == d:
        s += 0.06                                     # early burst: acceleration starting out of compression
    chase = m.chase_up if d > 0 else m.chase_dn
    allow = cfg.mb_max_chase_atr * (1.3 if m.regime == "fast" else 1.0)
    if chase > allow:
        vetoes.append(f"anti-chase: already {chase:.2f} ATR from the swing (max {allow:.2f})")
    elif chase > 0.7 * allow:
        s -= 0.25 * (chase - 0.7 * allow) / (0.3 * allow)
    if d * m.body > cfg.mb_max_bar_atr * (1.3 if m.regime == "fast" else 1.0):
        vetoes.append(f"anti-chase: candle already {d * m.body:.2f} ATR in that direction")
    if m.exhaustion == d:
        s -= 0.10                                     # t30u: exhaustion is a penalty, not a veto (no long waits)
    if m.rejection == -d:
        s -= 0.15
    if m.abnormal and d * m.accel < 0:
        s -= 0.10
    if m.absorption:
        s -= 0.10                                     # activity up but price not responding
    if m.weak_return and m.pb_dir == d:
        s -= 0.10
    if m.spread_target > cfg.mb_max_spread_target:
        vetoes.append(f"spread {m.spread_target:.2f}x the capture target (cost too high)")
    # t34u: positive evidence for THIS side is required (absence of the other side is not a signal), but it may
    # come from live tick force OR the forming M5/M1 bar momentum - not only from instantaneous tick velocity
    if d * m.vel < 0:
        vetoes.append("price moving against this side right now")
    elif c["velocity"] < cfg.mb_min_velocity and max(dd or 0.0, e or 0.0) < cfg.mb_scan_support:
        vetoes.append("no live force in that direction now (ticks, M1 and M5 momentum all weak)")
    if getattr(cfg, "precision_enabled", False) and m.prec is not None and getattr(m.prec, "ready", False):
        # t45u: chase / counter-structure are judged against the pullback zone and the M5 break (precision.py), not
        # against the last micro swing - a pullback entry is by definition far from that swing
        vetoes = [v for v in vetoes if not (v.startswith("anti-chase: already") or v.startswith("already spiked")
                                            or v.startswith("against the M5 structure"))]
    fam = (round(a, 3), round(b, 3), round(cs, 3), None if dd is None else round(dd, 3),
           None if e is None else round(e, 3), None if f is None else round(f, 3))
    return _clip(s), c, fam, vetoes


def decide(m: Micro, cfg) -> Strength:
    """BUY / SELL / NO TRADE from how well the families agree NOW. No single indicator is mandatory, but at
    least `mb_min_agree` families must support the side (2 when the M1/M5 families have no bars yet), price
    must be moving that way, and activity (B) is never enough on its own: no volume-only, no speed-only."""
    st = Strength()
    if not m.ready:
        st.reason = m.reason
        return st
    st.threshold = threshold(m, cfg)
    prec_on = bool(getattr(cfg, "precision_enabled", False))
    if prec_on:
        from . import precision
        if m.prec is not None:
            st.plans = {"BUY": precision.evaluate(m.prec, m, 1, cfg), "SELL": precision.evaluate(m.prec, m, -1, cfg)}
        else:
            st.plans = {"BUY": precision.Plan("BUY", vetoes=["no M5 structure data"]),
                        "SELL": precision.Plan("SELL", vetoes=["no M5 structure data"])}
        st.pe = {k: round(v.score, 3) for k, v in st.plans.items()}
    b, cb, sb, vb = side_strength(m, 1, cfg)
    s, cs, ss, vs = side_strength(m, -1, cfg)
    st.buy, st.sell = round(b, 3), round(s, 3)
    st.comps = {"BUY": {k: round(v, 2) for k, v in cb.items()}, "SELL": {k: round(v, 2) for k, v in cs.items()}}
    st.scan = {"BUY": dict(zip(FAMILIES, sb)), "SELL": dict(zip(FAMILIES, ss))}
    lvl = cfg.mb_scan_support
    st.support = {"BUY": sum(x is not None and x >= lvl for x in sb), "SELL": sum(x is not None and x >= lvl for x in ss)}
    if not m.stable:
        st.reason = f"feed unstable (gap {m.gap_max:.1f}s)"
        return st
    v1 = m.mtf
    if getattr(cfg, "ip_enabled", False) and getattr(cfg, "m1_only", False) and v1 is not None and v1.m1.ready:
        # t48u IMPULSE -> PULLBACK: the ONLY entry in M1 mode. M5 = trend direction, M1 = the impulse and the pullback that turned.
        bias5 = float(v1.m5_bias) if v1.m5.ready else float(v1.bias)
        res = {1: ip_eval(v1.m1, 1, bias5, cfg), -1: ip_eval(v1.m1, -1, bias5, cfg)}
        for d_, vet_ in ((1, vb), (-1, vs)):
            if res[d_][0] and any(x.startswith("spread") for x in vet_):
                res[d_] = (False, f"{'BUY' if d_ > 0 else 'SELL'}: {next(x for x in vet_ if x.startswith('spread'))}", {})
        pick = [d_ for d_ in (1, -1) if res[d_][0]]
        if pick:
            d_ = pick[0]
            ok_, why_, info_ = res[d_]
            q = min(1.0, 0.5 + 0.5 * float(info_.get("eff", 0.5)))
            st.side = "BUY" if d_ > 0 else "SELL"
            st.buy, st.sell = (q, 0.0) if d_ > 0 else (0.0, q)
            st.reason, st.ip_key, st.ip_stop, st.ip_info = why_, tuple(info_["key"]), float(info_["stop"]), info_
            st.origin = m.origin_up if d_ > 0 else m.origin_dn
            return st
        show = 1 if bias5 >= 0 else -1
        st.reason = f"IMPULSE-PULLBACK waiting (M5 {bias5:+.2f}) - {res[show][1]}"
        return st
    if getattr(cfg, "m1_only", False) and getattr(cfg, "m1_every_bar", False) and v1 is not None and v1.m1.ready:
        bias = float(v1.bias)
        bd = (1 if bias > 0 else -1) if abs(bias) >= 0.10 else (1 if b >= s else -1)
        st.bar_side = "BUY" if bd > 0 else "SELL"
        st.bar_reason = (f"M1 BAR ENTRY {st.bar_side}: M1 trend bias {bias:+.2f} (M1 indicators), BUY {b:.2f} vs SELL {s:.2f} "
                         f"- no threshold / agreement wait")
        if getattr(cfg, "sniper_enabled", False):
            # t47u: the bar entry is the SNIPER - only after the pullback wick (BUY: lower wick, SELL: upper wick) and not chasing
            ok_s, sc_s, txt_s = m1_sniper(v1.m1, bd, bias, cfg.sniper_min_wick, cfg.sniper_min_score, cfg.sniper_chase_atr)
            if ok_s:
                st.bar_reason = f"M1 SNIPER ENTRY (M1 bar open / wick): {txt_s}"
            else:
                st.bar_side, st.bar_reason = "", f"M1 sniper waiting: {txt_s}"
    # DUAL-SIDE: each side is checked on its OWN evidence (threshold, vetoes, agreement); a weak or vetoed
    # side never implies the other one and never blocks it. The side that passes must also beat the other
    # side's raw score by the edge (no SELL into a strong up-move just because BUY was vetoed for chasing).
    def check(d):
        best, other, sc, vet = (b, s, sb, vb) if d > 0 else (s, b, ss, vs)
        side = "BUY" if d > 0 else "SELL"
        if vet:
            return None, f"{side} {best:.2f}: " + "; ".join(vet)
        thr_d = st.threshold - (float(getattr(cfg, "pe_dir_relax", 0.0)) if prec_on else 0.0)
        if best < thr_d:
            return None, f"no edge: {side} {best:.2f} < threshold {thr_d:.2f} ({m.regime})"
        if best - other < cfg.mb_edge:
            return None, f"two-sided: BUY {b:.2f} vs SELL {s:.2f}"
        avail = [f for f, x in zip(FAMILIES, sc) if x is not None]
        agree = [f for f, x in zip(FAMILIES, sc) if x is not None and x >= lvl]
        need = max(2, min(cfg.mb_min_agree, len(avail)))
        # t32u: once M1 bars exist the M1 trigger (D) is REQUIRED; before that micro-structure (C) triggers
        trig_fams = ("D",) if (cfg.mb_require_m1_trigger and "D" in avail) else ("C", "D", "E")
        trigger = [f for f in agree if f in trig_fams]
        if len(agree) < need or not trigger:
            return None, (f"{side} {best:.2f} but only {len(agree)}/{len(avail)} families agree "
                          f"({''.join(agree) or '-'}), need {need} incl. the trigger ({'/'.join(trig_fams)}) - no "
                          f"single-indicator, speed-only or volume-only entries")
        if prec_on:
            # t45u: right direction is not enough - the LOCATION must pass (structure, confluence, trigger, stop, R:R)
            pl = st.plans[side]
            if pl.vetoes or not pl.ready:
                return None, (f"{side} direction {best:.2f} ok but location rejected (position score {pl.score:.2f}): "
                              + "; ".join(pl.vetoes[:2]))
            if pl.score < float(getattr(cfg, "pe_min", 0.58)):
                return None, f"{side} direction {best:.2f} ok but position score {pl.score:.2f} < {getattr(cfg, 'pe_min', 0.58):.2f}"
        tag = " early-burst" if m.early == d else ""
        fams = " ".join(f"{f}{x:.2f}" for f, x in zip(FAMILIES, sc) if x is not None)
        pe_txt = ""
        if prec_on:
            pl = st.plans[side]
            pe_txt = (f" | PE {pl.score:.2f} {pl.kind} {pl.path} depth {pl.depth:.2f} [{'+'.join(pl.tags)}] R:R {pl.rr:.1f} "
                      f"-> {pl.target_kind} {pl.target:.5g}")
        return best, f"{side} {best:.2f} >= {st.threshold:.2f} [{m.regime}{tag}] agree {''.join(agree)} | {fams}{pe_txt}"
    first = 1 if b >= s else -1
    res = {d: check(d) for d in (first, -first)}
    ok = [d for d in (first, -first) if res[d][0] is not None]
    if not ok:
        st.vetoes = vb if first > 0 else vs
        st.reason = res[first][1] + ("" if res[-first][1] == res[first][1] else f" | {res[-first][1]}")
        return st
    d = ok[0]
    if (getattr(cfg, "sniper_enabled", False) and getattr(cfg, "m1_only", False) and v1 is not None and v1.m1.ready):
        # t47u: a threshold signal is also taken only on the M1 wick (no chasing the break of the M1 low / high)
        ok_s, sc_s, txt_s = m1_sniper(v1.m1, d, float(v1.bias), cfg.sniper_min_wick, cfg.sniper_min_score, cfg.sniper_chase_atr)
        if not ok_s:
            st.reason = f"{res[d][1]} | M1 sniper waiting: {txt_s}"
            return st
    st.side = "BUY" if d > 0 else "SELL"
    st.origin = m.origin_up if d > 0 else m.origin_dn
    st.reason = res[d][1]
    return st


# ================================================================== ONE basket exit (momentum hold)
def exit_signals(g: dict, m: Micro, d: int, opp: float, thr: float, cfg) -> dict:
    """The exhaustion / reversal evidence for a basket in direction d (each True/False). Read together - no
    single one closes a basket that still has momentum."""
    atr = m.atr or 1e-9
    vel = d * m.vel
    flip = vel <= -max(cfg.mb_flip_atr, 0.5 * m.spread / atr)
    pk_v = float(g.get("peak_vel", 0.0))
    decay = pk_v > 0 and vel < cfg.mb_decay_frac * pk_v
    opp_press = d * m.imbalance <= -cfg.mb_opp_pressure or d * m.bidask <= -cfg.mb_opp_pressure
    stall = (m.t - float(g.get("last_ext_t", m.t))) >= cfg.mb_stall_seconds * (0.6 if m.regime == "fast" else 1.0)
    v1 = m.mtf.m1 if m.mtf is not None and m.mtf.m1.ready else None
    rsi_turn = (v1 is not None and v1.rsi_ready and d * v1.rsi_slope < -cfg.exit_rsi_turn
                and (d * v1.rsi_accel < 0 or d * (v1.rsi - 50.0) > 20.0))
    macd_turn = v1 is not None and d * v1.hist_slope < -0.03 and d * v1.hist_accel < 0
    candle_loss = (decay and stall) or (v1 is not None and d * v1.body < -0.15)
    vol_fade = v1 is not None and v1.vol_x is not None and v1.vol_x < 0.7 and (decay or stall)
    reversal = (flip and opp_press) or opp >= cfg.exit_opp_frac * thr
    struct_fail = m.failed == -d or m.struct_break == -d or (m.rejection == -d and flip)
    exhaustion = m.exhaustion == d or (v1 is not None and v1.exhaust == d)
    return {"RSI-turn": rsi_turn, "MACD-hist-turn": macd_turn, "candle-momentum-loss": candle_loss,
            "volume-weakening": vol_fade, "reversal": reversal, "structure-failure": struct_fail,
            "exhaustion": exhaustion, "_continuing": (vel > 0 and not decay and not opp_press and not macd_turn
                                                      and d * m.accel > -0.5 * cfg.mb_vel_ref_atr)}


def exit_reason(g: dict, m: Micro, d: int, pl: float, held: float, opp: float, thr: float,
                stop_dist: float, cost: float, cfg) -> Optional[str]:
    """Momentum hold + intelligent exit on NET basket P/L (pl = executable P/L per unit in price after the
    spread; cost = other per-unit costs such as commission). There is NO fixed take-profit: the basket is held
    while the main momentum continues; profit is protected by the break-even / dynamic trailing stop (trader)
    and closed here when the exhaustion evidence AGREES. Hard risk exits (hard stop, safe mode, exposure,
    emergency) are checked by the trader BEFORE this and always win."""
    if not m.ready:
        return None
    net = pl - cost
    atr = m.atr or 1e-9
    min_net = cfg.mb_min_net_spread * m.spread
    sig = exit_signals(g, m, d, opp, thr, cfg)
    continuing = sig.pop("_continuing")
    hits = [k for k, v in sig.items() if v]
    score = len(hits)
    tags = "+".join(hits) or "none"
    peak = float(g.get("peak_pl", 0.0))
    # meaningful retracement: noise-aware (ATR and spread based), never a few points
    retr = peak - net
    retr_min = max(cfg.exit_retrace_atr * atr, cfg.exit_retrace_spread * m.spread)
    retrace = peak > min_net and retr >= retr_min and retr >= cfg.exit_retrace_frac * peak
    flip = d * m.vel <= -max(cfg.mb_flip_atr, 0.5 * m.spread / atr)
    press = d * m.imbalance <= -cfg.mb_opp_pressure or d * m.bidask <= -cfg.mb_opp_pressure
    wick = m.rejection == -d
    v = m.mtf
    trend_with = v is not None and v.m1.ready and d * v.bias >= 0.25
    # M1 swing (HL/LH) broken against us AFTER the entry (t34u: a reversal entry taken while the old swing was
    # already broken is not "invalidated" by that same old break - its structure stop protects it)
    m1_broke = v is not None and v.m1.ready and v.m1.sbreak == -d and g.get("sb0") != -d
    # t32u: structure intact = M1 trend with us (or not against) and no M1 swing break / micro structure failure
    intact = (not m1_broke) and not sig["structure-failure"] and (trend_with or v is None or not v.m1.ready
                                                                  or d * v.bias > -0.25)
    # 1. in profit: hold while momentum continues; close when the evidence agrees
    # t32u: a discretionary profit exit must be worth it against the risk taken - below exit_min_r x stop the
    # basket is left to break-even / trail / structure (no +0.3 exits on a 4.0 stop)
    small = cfg.exit_min_r > 0 and stop_dist > 0 and net < cfg.exit_min_r * stop_dist and not m1_broke
    if net >= min_net and small and not (g.get("prot") and sig["reversal"] and sig["structure-failure"]):
        return None
    if net >= min_net:
        # t32u (7): once the trailing stop is active and the structure is intact the TRAIL manages the exit -
        # no premature spike / exhaustion close in a running trend
        if g.get("prot") == "trail" and intact and trend_with:
            return None
        # spike exit: after a real spike the M1 bar starts turning back (velocity flip + opposing tick pressure or
        # a growing wick against us + a retrace of the spike) -> take it now; the engine re-evaluates at once
        if (peak >= cfg.exit_spike_atr * atr and retr >= cfg.exit_spike_wick_frac * peak and flip
                and (press or wick)):
            return (f"spike turning back: peak {peak:+.4g} -> {net:+.4g} (velocity flip"
                    f"{' + opposing pressure' if press else ''}{' + wick against' if wick else ''}, {tags})")
        if score >= cfg.exit_min_signals and not continuing:
            return f"momentum exhausted in profit ({score} signals: {tags}): {net:+.4g}"
        if trend_with and intact and retr < 0.6 * peak and not (flip and press):
            return None                     # pullback inside the M1 trend: hold (the trailing stop protects)
        if retrace and (score >= cfg.exit_min_signals - 1 or (score >= 1 and not continuing)):
            return (f"profit protection: retraced {retr:.4g} from peak {peak:+.4g} with {tags} "
                    f"(noise floor {retr_min:.4g}): {net:+.4g}")
        if sig["reversal"] and sig["structure-failure"]:
            return f"directional reversal in profit ({tags}): {net:+.4g}"
        return None
    # 2. not in net profit. t32u: losers are NOT cut on momentum noise any more (that cut -1.7..-4.4 baskets that
    # then went our way). They are held inside the structure stop while the M1 structure is intact and closed
    # only when the structure that justified the entry is invalidated (or by the structure stop at the broker).
    if m1_broke:
        return f"structure invalidated: M1 swing broken against the basket ({tags}): {net:+.4g}"
    if (sig["structure-failure"] and sig["reversal"] and not trend_with and score >= cfg.exit_min_signals
            and peak >= min_net and retrace):
        return f"profit gone + structure failed ({tags}): peak {peak:+.4g} -> {net:+.4g}"
    if (sig["structure-failure"] and sig["reversal"] and v is not None and v.m1.ready and d * v.bias <= -0.25
            and score >= cfg.exit_min_signals):
        return f"structure invalidated: micro structure failed against the M1 trend ({tags}): {net:+.4g}"
    if (v is None or not v.m1.ready) and sig["structure-failure"] and sig["reversal"] and score >= 3:
        # no M1 bars yet: the micro structure is the structure - broad reversal through it invalidates the trade
        return f"structure invalidated: micro structure failed with a broad reversal ({score} signals: {tags}): {net:+.4g}"
    if cfg.mb_cut_frac > 0 and stop_dist > 0 and net <= -cfg.mb_cut_frac * stop_dist and score >= 2 and not continuing:
        return f"momentum against: {net:+.4g} (>= {cfg.mb_cut_frac:g} of stop) {tags}"   # legacy, off by default
    if cfg.mb_max_hold_seconds > 0 and held >= cfg.mb_max_hold_seconds and not continuing and score >= 1 and not intact:
        return f"stale: {held:.0f}s without net profit ({net:+.4g}, {tags})"
    return None
