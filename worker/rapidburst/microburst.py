"""Ultra-Fast Pre-Burst Short Scalping Engine: scanners, decision and the basket exit (pure logic).

ONE engine. Three scanners run on the SAME real tick stream and feed ONE decision:
  A. tick velocity & acceleration  - tick rate, velocity, acceleration/deceleration, direction sequence,
                                     directional efficiency, early acceleration, exhaustion
  B. activity burst & candle pressure - relative tick activity, activity expansion, intrabar body expansion,
                                     body/range, wick rejection, consecutive pressure, activity vs price
                                     response, absorption
  C. micro-structure pre-burst trigger - micro high/low, compression -> expansion, micro breakout, failed
                                     breakout, structure break, pullback recovery, short rebound, anti-chase

Speed: every measurement is kept INCREMENTALLY by TickScanner.push() (rolling windows with running sums and
monotonic min/max deques, O(1) amortised per tick). Reading a snapshot never rescans the tick history.

Honesty notes (also shown in the UI):
  * "tick volume"/"activity" = number of price updates the broker sent. It is NOT exchange volume.
  * "flow"/"imbalance" = up- vs down-ticks and bid/ask changes: a PROXY for pressure, not real order flow.
  * Market depth is not used: neither TickerAll nor this MT5 bridge exposes a reliable book.
No RSI or other lagging oscillator, no fixed confirmation steps, no waiting for the candle close.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from statistics import median
from typing import Optional


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
        m.stable = m.gap_max <= cfg.mb_max_gap_s and m.latency <= cfg.mb_max_gap_s
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

    @property
    def score(self) -> float:
        return self.buy if self.side == "BUY" else -self.sell if self.side == "SELL" else (self.buy - self.sell)


SCAN_W = {"fast": (1.3, 0.9, 0.8), "normal": (1.0, 1.0, 1.0), "quiet": (0.8, 0.8, 1.3), "chop": (0.8, 1.0, 1.2)}


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
    c = _components(m, d, cfg)
    a, b, cs = _scanners(c)
    wa, wb, wc = SCAN_W.get(m.regime, SCAN_W["normal"])
    s = (wa * a + wb * b + wc * cs) / (wa + wb + wc)
    vetoes = []
    if m.early == d:
        s += 0.06                                     # pre-burst: acceleration starting out of compression
    chase = m.chase_up if d > 0 else m.chase_dn
    allow = cfg.mb_max_chase_atr * (1.3 if m.regime == "fast" else 1.0)
    if chase > allow:
        vetoes.append(f"anti-chase: already {chase:.2f} ATR from the swing (max {allow:.2f})")
    elif chase > 0.7 * allow:
        s -= 0.25 * (chase - 0.7 * allow) / (0.3 * allow)
    if d * m.body > cfg.mb_max_bar_atr * (1.3 if m.regime == "fast" else 1.0):
        vetoes.append(f"anti-chase: candle already {d * m.body:.2f} ATR in that direction")
    if m.exhaustion == d:
        vetoes.append("exhaustion: extended move is decelerating")
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
    if d * m.vel <= 0 or c["velocity"] < cfg.mb_min_velocity:
        vetoes.append("no live force in that direction now")
    return _clip(s), c, (round(a, 3), round(b, 3), round(cs, 3)), vetoes


def decide(m: Micro, cfg) -> Strength:
    """BUY / SELL / NO TRADE from the force that is starting NOW. Needs scanner A (live tick force) AND
    scanner B or C (activity/candle pressure or a structure trigger): never volume-only, never speed-only."""
    st = Strength()
    if not m.ready:
        st.reason = m.reason
        return st
    st.threshold = threshold(m, cfg)
    b, cb, sb, vb = side_strength(m, 1, cfg)
    s, cs, ss, vs = side_strength(m, -1, cfg)
    st.buy, st.sell = round(b, 3), round(s, 3)
    st.comps = {"BUY": {k: round(v, 2) for k, v in cb.items()}, "SELL": {k: round(v, 2) for k, v in cs.items()}}
    st.scan = {"BUY": dict(zip("ABC", sb)), "SELL": dict(zip("ABC", ss))}
    lvl = cfg.mb_scan_support
    st.support = {"BUY": sum(x >= lvl for x in sb), "SELL": sum(x >= lvl for x in ss)}
    if not m.stable:
        st.reason = f"feed unstable (gap {m.gap_max:.1f}s)"
        return st
    d = 1 if b >= s else -1
    best, other, sc, vet = (b, s, sb, vb) if d > 0 else (s, b, ss, vs)
    side = "BUY" if d > 0 else "SELL"
    st.vetoes = vet
    if vet:
        st.reason = f"{side} {best:.2f}: " + "; ".join(vet)
        return st
    if best < st.threshold:
        st.reason = f"no edge: {side} {best:.2f} < threshold {st.threshold:.2f} ({m.regime})"
        return st
    if best - other < cfg.mb_edge:
        st.reason = f"two-sided: BUY {b:.2f} vs SELL {s:.2f}"
        return st
    if sc[0] < lvl:
        st.reason = f"{side} {best:.2f} but scanner A (tick force) only {sc[0]:.2f} - no volume/structure-only entries"
        return st
    if sc[1] < lvl and sc[2] < lvl:
        st.reason = (f"{side} {best:.2f} but only speed: B {sc[1]:.2f} / C {sc[2]:.2f} < {lvl:.2f} "
                     f"- no speed-only entries")
        return st
    st.side = side
    st.origin = m.origin_up if d > 0 else m.origin_dn
    tag = " pre-burst" if m.early == d else ""
    st.reason = (f"{side} {best:.2f} >= {st.threshold:.2f} [{m.regime}{tag}] A{sc[0]:.2f} B{sc[1]:.2f} C{sc[2]:.2f}")
    return st


# ================================================================== ONE basket exit
def exit_reason(g: dict, m: Micro, d: int, pl: float, held: float, opp: float, thr: float,
                stop_dist: float, cost: float, cfg) -> Optional[str]:
    """Basket short profit capture (pl = executable P/L per unit in price after the spread; cost = other per-unit
    costs such as commission). Hard risk exits are checked by the trader BEFORE this and always win."""
    if not m.ready:
        return None
    net = pl - cost
    atr = m.atr
    min_net = cfg.mb_min_net_spread * m.spread
    target = max(g.get("target", 0.0), m.target * 0.5) or m.target
    vel = d * m.vel
    flip = vel <= -max(cfg.mb_flip_atr, 0.5 * m.spread / atr if atr else 0.0)
    pk_v = float(g.get("peak_vel", 0.0))
    decay = pk_v > 0 and vel < cfg.mb_decay_frac * pk_v
    opp_press = d * m.imbalance <= -cfg.mb_opp_pressure or d * m.bidask <= -cfg.mb_opp_pressure
    stall = (m.t - float(g.get("last_ext_t", m.t))) >= cfg.mb_stall_seconds * (0.6 if m.regime == "fast" else 1.0)
    exhausted = m.exhaustion == d
    continuing = vel > 0 and not decay and not opp_press and not exhausted and d * m.accel > -0.5 * cfg.mb_vel_ref_atr
    peak = float(g.get("peak_pl", 0.0))
    tags = "+".join(k for k, v in (("velocity-decay", decay), ("tick-flip", flip), ("opposing-pressure", opp_press),
                                   ("stall", stall), ("exhaustion", exhausted)) if v)
    # 1. profit capture on NET basket P/L
    if net >= target * cfg.mb_full_target_mult:
        return f"profit capture: {net:+.4g} >= {cfg.mb_full_target_mult:g}x target (short capture, no long hold)"
    if net >= target and not continuing:
        return f"profit capture: {net:+.4g} (target {target:.4g}) {tags or 'momentum fading'}"
    if peak >= 0.6 * target:
        gb = cfg.mb_giveback if not continuing else cfg.mb_giveback_running
        if net <= peak * (1.0 - gb) and net >= min_net:
            return (f"profit protection (give-back): {net:+.4g} gave back {1 - net / peak:.0%} of peak {peak:+.4g} "
                    f"(limit {gb:.0%}{' while running' if continuing else ''})")
        if peak >= target and net < min_net:
            return f"profit protection: back to {net:+.4g} after peak {peak:+.4g}"
    if net >= min_net and exhausted:
        return f"momentum exhaustion in profit: {net:+.4g}"
    if net >= min_net and flip and opp_press:
        return f"tick reversal in profit: {net:+.4g} {tags}"
    # 2. micro-structure failure / reversal (any P/L)
    if m.failed == -d:
        return f"failed breakout against the basket ({net:+.4g})"
    if m.struct_break == -d:
        return f"micro structure break against the basket ({net:+.4g})"
    if m.rejection == -d and flip:
        return f"wick rejection + tick flip ({net:+.4g})"
    if opp >= thr:
        return f"micro-reversal: opposite strength {opp:.2f} >= {thr:.2f} ({net:+.4g})"
    # 3. no follow-through / momentum against / stale
    ft = cfg.mb_follow_seconds * {"fast": 0.6, "normal": 1.0, "quiet": 1.5, "chop": 0.8}[m.regime]
    if held >= ft and peak < min_net and (vel <= 0 or opp_press):
        return f"no follow-through in {held:.1f}s (best {peak:+.4g}, now {net:+.4g})"
    if stop_dist > 0 and net <= -cfg.mb_cut_frac * stop_dist and (flip or opp_press):
        return f"momentum against: {net:+.4g} (>= {cfg.mb_cut_frac:g} of stop) {tags}"
    if cfg.mb_max_hold_seconds > 0 and held >= cfg.mb_max_hold_seconds and net < min_net:
        return f"stale: {held:.0f}s without net profit ({net:+.4g})"
    return None
