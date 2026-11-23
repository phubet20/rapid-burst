"""Rapid Micro-Burst Scalping Engine: analysis, BUY/SELL strength and the tick-based exit (pure logic).

ONE engine. It replaces the old momentum score, the M1 impulse detectors, the burst/basket/scalp modes
and their separate TP/exit systems. Inputs are REAL ticks only (bid/ask/time) plus the broker's M1 bars
for context (ATR, previous candles). No RSI or other lagging oscillator, no fixed confirmation chain,
no waiting for the candle close.

Honesty notes (also shown in the UI):
  * "tick volume" = number of price updates the broker sent. It is NOT exchange volume and NOT evidence
    of who bought or sold.
  * "flow"/"imbalance" = up-ticks vs down-ticks and bid/ask changes: a PROXY for pressure, not real
    order flow.
  * Market depth is not used: neither TickerAll nor this MT5 bridge exposes a reliable book.
"""
from __future__ import annotations

import math
from bisect import bisect_right
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
                "gap": r(self.gap_max, 1), "target": self.target, "atr": self.atr, "spread": self.spread}


class _Series:
    def __init__(self, ticks):
        self.ticks = ticks
        self.times = [tk.t for tk in ticks]

    def at(self, t: float):
        i = bisect_right(self.times, t) - 1
        return self.ticks[i] if i >= 0 else None

    def mid_at(self, t: float, default: float) -> float:
        tk = self.at(t)
        return tk.mid if tk is not None else default

    def window(self, t0: float, t1: float) -> list:
        return self.ticks[bisect_right(self.times, t0):bisect_right(self.times, t1)]


def _rng(ticks) -> float:
    if not ticks:
        return 0.0
    m = [x.mid for x in ticks]
    return max(m) - min(m)


def analyze(ticks: list, now: float, atr: float, vu: float, spreads, bars: list, cfg,
            slip_hist: Optional[list] = None) -> Micro:
    """All micro-structure measurements from REAL ticks (and the broker's completed M1 bars)."""
    m = Micro()
    if not ticks:
        m.reason = "no live ticks"
        return m
    last = ticks[-1]
    m.t, m.mid, m.bid, m.ask, m.spread = last.t, last.mid, last.bid, last.ask, last.ask - last.bid
    m.latency = max(0.0, now - last.t)
    span = last.t - ticks[0].t
    if len(ticks) < cfg.min_ticks_warmup or span < cfg.warmup_seconds:
        m.reason = f"warming up: {len(ticks)}/{cfg.min_ticks_warmup} ticks, {span:.1f}/{cfg.warmup_seconds:.1f}s"
        return m
    if m.latency > cfg.max_tick_age:
        m.reason = f"market idle (last tick {m.latency:.0f}s ago)"
        return m
    S = _Series(ticks)
    vu = max(vu, 1e-12)
    atr = atr if atr > 0 else 4.0 * vu
    m.atr, m.vu = atr, vu
    now = last.t if now - last.t < 1.0 else now

    # ---- A. tick flow & micro momentum
    w5 = S.window(now - 5.0, now)
    m.tick_rate = len(w5) / 5.0
    w60 = S.window(now - 60.0, now)
    base_span = max(5.0, min(60.0, now - ticks[0].t))
    base_rate = len(w60) / base_span
    m.rate_ratio = m.tick_rate / base_rate if base_rate > 0 else 1.0
    W = cfg.mb_vel_seconds
    m0, m1, m2 = last.mid, S.mid_at(now - W, ticks[0].mid), S.mid_at(now - 2 * W, ticks[0].mid)
    m.vel, m.vel_prev = (m0 - m1) / atr, (m1 - m2) / atr
    m.accel = m.vel - m.vel_prev
    seq5 = ([S.at(now - 5.0)] if S.at(now - 5.0) else []) + w5
    dif = [b.mid - a.mid for a, b in zip(seq5, seq5[1:])]
    s_abs = sum(abs(x) for x in dif)
    m.eff = (sum(dif) / s_abs) if s_abs > 0 else 0.0
    m3 = m0 - S.mid_at(now - 3.0, ticks[0].mid)
    m.burst_z, m.burst_dir = abs(m3) / vu, _sgn(m3)
    ch = [x for x in (b.mid - a.mid for a, b in zip(ticks[-40:], ticks[-39:])) if x != 0]
    if ch:
        sgn = [_sgn(x) for x in ch[-12:]]
        m.seq_consist = sum(sgn) / len(sgn)
        run = 0
        for x in reversed(sgn):
            if run == 0 or _sgn(run) == x:
                run += x
            else:
                break
        m.seq_run = run

    # ---- B. tick volume = activity count (proxy only)
    m.act_ratio = m.rate_ratio
    m5 = abs(m0 - S.mid_at(now - 5.0, ticks[0].mid))
    m.absorption = m.act_ratio >= cfg.mb_absorb_activity and m5 < 0.5 * vu and abs(m.eff) < 0.3

    # ---- C. live candle (from ticks, no close wait) + previous closed broker bars
    bs = math.floor(now / 60.0) * 60.0
    inbar = S.window(bs - 1e-9, now)
    o = S.mid_at(bs, inbar[0].mid if inbar else m0)
    mids = [x.mid for x in inbar] + [o]
    hi, lo, c = max(mids), min(mids), m0
    rng = hi - lo
    m.bar_age = now - bs
    m.body = (c - o) / atr
    m.range_atr = rng / atr
    if rng > 0:
        m.body_range = abs(c - o) / rng
        m.upper_wick = (hi - max(o, c)) / rng
        m.lower_wick = (min(o, c) - lo) / rng
        m.close_loc = (c - lo) / rng
    prev = [b for b in (bars or []) if b.get("high") is not None and b.get("time", 0) < bs][-6:]
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

    # ---- D. micro structure
    st = S.window(now - cfg.mb_struct_seconds, now - 3.0)
    buf = max(0.1 * vu, 0.5 * m.spread)
    if st:
        m.micro_hi, m.micro_lo = max(x.mid for x in st), min(x.mid for x in st)
        if m0 > m.micro_hi + buf:
            m.breakout = 1
        elif m0 < m.micro_lo - buf:
            m.breakout = -1
        rec = S.window(now - 10.0, now)
        if rec:
            rh, rl = max(x.mid for x in rec), min(x.mid for x in rec)
            if rh > m.micro_hi + buf and m0 < m.micro_hi - buf:
                m.failed = -1                      # broke above, back inside the range -> bearish
            elif rl < m.micro_lo - buf and m0 > m.micro_lo + buf:
                m.failed = 1
    d20 = m0 - S.mid_at(now - 20.0, ticks[0].mid)
    last10 = S.window(now - 10.0, now - 2.0)
    if last10 and abs(d20) >= 1.0 * vu:
        if d20 > 0 and m0 < min(x.mid for x in last10) - buf:
            m.struct_break = -1
        elif d20 < 0 and m0 > max(x.mid for x in last10) + buf:
            m.struct_break = 1
    blocks, t = [], max(ticks[0].t, now - 120.0)
    while t + 5.0 <= now:
        r = _rng(S.window(t, t + 5.0))
        if r > 0:
            blocks.append(r)
        t += 5.0
    r5 = max(median(blocks) if blocks else vu, 1e-12)
    m.compressed = _rng(S.window(now - 15.0, now - 3.0)) < cfg.mb_compress_ratio * r5
    m.expansion = _rng(S.window(now - 3.0, now) + ([S.at(now - 3.0)] if S.at(now - 3.0) else [])) / r5

    # ---- E. pullback & continuation, anti-chase distances
    p30 = S.mid_at(now - 30.0, ticks[0].mid)
    tr = m0 - p30
    if abs(tr) >= 1.5 * vu:
        s = _sgn(tr)
        w30 = S.window(now - 30.0, now)
        ext_i = max(range(len(w30)), key=lambda i: s * w30[i].mid) if w30 else 0
        if w30:
            ext = w30[ext_i].mid
            after = w30[ext_i:]
            trough = min((x.mid for x in after), key=lambda x: s * x)
            leg = (ext - p30) * s
            m.pb_dir = s
            m.pb_depth = ((ext - trough) * s / leg) if leg > 0 else 0.0
            off = (m0 - trough) * s
            if 0.15 <= m.pb_depth <= 0.7 and s * m.vel > 0 and s * m.accel >= 0 and off >= 0.3 * vu:
                m.continuation = s
            elif m.pb_depth > 0.15 and off < 0.3 * (ext - trough) * s and s * m.vel <= 0:
                m.weak_return = True
    sw = S.window(now - cfg.mb_swing_seconds, now) or [last]
    lo_tk = min(sw, key=lambda x: x.mid)
    hi_tk = max(sw, key=lambda x: x.mid)
    m.chase_up, m.origin_up = (m0 - lo_tk.mid) / atr, lo_tk.t
    m.chase_dn, m.origin_dn = (hi_tk.mid - m0) / atr, hi_tk.t

    # ---- F. pressure proxy (tick direction + bid/ask changes; NOT real order flow)
    w3 = S.window(now - cfg.mb_flow_seconds, now)
    seq3 = ([S.at(now - cfg.mb_flow_seconds)] if S.at(now - cfg.mb_flow_seconds) else []) + w3
    up = sum(1 for a, b in zip(seq3, seq3[1:]) if b.mid > a.mid)
    dn = sum(1 for a, b in zip(seq3, seq3[1:]) if b.mid < a.mid)
    m.imbalance = (up - dn) / (up + dn) if up + dn else 0.0
    ba = 0
    tot = 0
    for a, b in zip(seq3, seq3[1:]):
        for x, y in ((a.bid, b.bid), (a.ask, b.ask)):
            if y != x:
                ba += 1 if y > x else -1
                tot += 1
    m.bidask = ba / tot if tot else 0.0

    # ---- G. volatility & execution context
    sp = list(spreads)[-300:] if spreads else []
    med = median(sp) if len(sp) >= 10 else m.spread
    m.spread_ratio = m.spread / med if med > 0 else 1.0
    gaps = [b.t - a.t for a, b in zip(S.window(now - 10.0, now), S.window(now - 10.0, now)[1:])]
    m.gap_max = max(gaps + [m.latency]) if gaps else m.latency
    m.stable = m.gap_max <= cfg.mb_max_gap_s and m.latency <= cfg.mb_max_gap_s
    fast = m.rate_ratio >= 1.6 and (m.burst_z >= 2.0 or m.range_exp >= 1.2)
    if fast:
        m.regime = "fast"
    elif m.tick_rate < cfg.min_tick_rate or vu < 0.03 * atr:
        m.regime = "quiet"
    elif abs(m.eff) < 0.25 and _rng(S.window(now - 30.0, now)) > 0.8 * atr and abs(tr) < 0.3 * atr:
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


@dataclass
class Strength:
    buy: float = 0.0
    sell: float = 0.0
    threshold: float = 1.0
    side: str = "NO TRADE"
    reason: str = ""
    origin: float = 0.0
    comps: dict = field(default_factory=dict)
    support: dict = field(default_factory=dict)
    vetoes: list = field(default_factory=list)

    @property
    def score(self) -> float:
        return self.buy if self.side == "BUY" else -self.sell if self.side == "SELL" else (self.buy - self.sell)


BASE_W = {"velocity": 1.0, "accel": 0.7, "efficiency": 0.8, "burst": 0.8, "flow": 0.8, "sequence": 0.5,
          "candle": 0.6, "structure": 0.9, "activity": 0.4}
REGIME_W = {"fast": {"velocity": 1.3, "accel": 1.0, "burst": 1.1, "structure": 0.7},
            "chop": {"efficiency": 1.2, "structure": 1.2, "velocity": 0.8, "burst": 0.6},
            "quiet": {"structure": 1.3, "activity": 0.6, "burst": 0.6},
            "normal": {}}
FAMILIES = ("velocity", "accel", "efficiency", "burst", "flow", "sequence", "candle", "structure")


def _components(m: Micro, d: int, cfg) -> dict:
    cdir = _sgn(m.body)
    candle = (0.4 * _clip(d * cdir * m.body_range) + 0.4 * _clip(d * (m.close_loc - 0.5) * 2)
              + 0.2 * (1.0 if d * m.consec >= 2 else 0.0))
    struct = 0.0
    if m.breakout == d:
        struct = max(struct, 1.0 if m.compressed else 0.7)
    if m.failed == d:
        struct = max(struct, 0.8)
    if m.struct_break == d:
        struct = max(struct, 0.7)
    if m.continuation == d:
        struct = max(struct, 0.9)
    return {"velocity": _clip(d * m.vel / cfg.mb_vel_ref_atr),
            "accel": _clip(d * m.accel / (0.6 * cfg.mb_vel_ref_atr)),
            "efficiency": _clip(d * m.eff),
            "burst": _clip((m.burst_z - 1.0) / 2.0) if m.burst_dir == d else 0.0,
            "flow": 0.5 * _clip(d * m.imbalance) + 0.5 * _clip(d * m.bidask),
            "sequence": 0.5 * _clip(d * m.seq_run / 5.0) + 0.5 * _clip(d * m.seq_consist),
            "candle": _clip(candle),
            "structure": struct,
            "activity": _clip((m.act_ratio - 1.0) / 1.0)}


def threshold(m: Micro, cfg) -> float:
    adj = {"fast": -0.05, "normal": 0.0, "quiet": 0.05, "chop": 0.08}[m.regime]
    return _clip(cfg.mb_threshold + adj + 0.12 * max(0.0, m.spread_ratio - 1.2) + 0.15 * m.slip_risk, 0.2, 0.95)


def side_strength(m: Micro, d: int, cfg) -> tuple[float, dict, int, list]:
    w = dict(BASE_W)
    w.update(REGIME_W.get(m.regime, {}))
    c = _components(m, d, cfg)
    s = sum(w[k] * c[k] for k in w) / sum(w.values())
    vetoes = []
    chase = m.chase_up if d > 0 else m.chase_dn
    allow = cfg.mb_max_chase_atr * (1.3 if m.regime == "fast" else 1.0)   # fast bursts travel further per tick
    if chase > allow:
        vetoes.append(f"anti-chase: already {chase:.2f} ATR from the swing (max {allow:.2f})")
    elif chase > 0.7 * allow:
        s -= 0.25 * (chase - 0.7 * allow) / (0.3 * allow)
    if d * m.body > cfg.mb_max_bar_atr * (1.3 if m.regime == "fast" else 1.0):
        vetoes.append(f"anti-chase: candle already {d * m.body:.2f} ATR in that direction")
    if m.rejection == -d:
        s -= 0.15
    if m.abnormal and d * m.accel < 0:
        s -= 0.10                                     # extended bar losing force = exhaustion
    if m.absorption:
        s -= 0.10
    if m.weak_return and m.pb_dir == d:
        s -= 0.10
    if m.spread_target > cfg.mb_max_spread_target:
        vetoes.append(f"spread {m.spread_target:.2f}x the capture target (cost too high)")
    if d * m.vel <= 0 or c["velocity"] < cfg.mb_min_velocity:
        vetoes.append("no live force in that direction now")
    support = sum(1 for k in FAMILIES if c[k] >= cfg.mb_support_level)
    return _clip(s), c, support, vetoes


def decide(m: Micro, cfg) -> Strength:
    """BUY / SELL / NO TRADE from the current (not past) force. Signals support each other; none is mandatory
    on its own, but a decision needs live velocity, enough independent families and a clear edge."""
    st = Strength()
    if not m.ready:
        st.reason = m.reason
        return st
    st.threshold = threshold(m, cfg)
    b, cb, nb, vb = side_strength(m, 1, cfg)
    s, cs, ns, vs = side_strength(m, -1, cfg)
    st.buy, st.sell = round(b, 3), round(s, 3)
    st.comps = {"BUY": {k: round(v, 2) for k, v in cb.items()}, "SELL": {k: round(v, 2) for k, v in cs.items()}}
    st.support = {"BUY": nb, "SELL": ns}
    if not m.stable:
        st.reason = f"feed unstable (gap {m.gap_max:.1f}s)"
        return st
    d = 1 if b >= s else -1
    best, other, sup, vet = (b, s, nb, vb) if d > 0 else (s, b, ns, vs)
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
    if sup < cfg.mb_min_support:
        st.reason = f"{side} {best:.2f} but only {sup} supporting signal families (< {cfg.mb_min_support})"
        return st
    top = sorted(((k, v) for k, v in (cb if d > 0 else cs).items() if k in FAMILIES), key=lambda x: -x[1])[:4]
    st.side = side
    st.origin = m.origin_up if d > 0 else m.origin_dn
    st.reason = (f"micro-burst {side} {best:.2f} >= {st.threshold:.2f} [{m.regime}] "
                 + "+".join(f"{k}{v:.1f}" for k, v in top))
    return st


def exit_reason(g: dict, m: Micro, d: int, pl: float, held: float, opp: float, thr: float,
                stop_dist: float, cost: float, cfg) -> Optional[str]:
    """ONE tick-based exit for a group (pl = executable P/L per unit in price, already after the spread;
    cost = other per-unit costs, e.g. commission). Profit capture, momentum exit, micro-reversal/failure.
    Hard risk exits are checked by the trader BEFORE this and always win."""
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
    peak = float(g.get("peak_pl", 0.0))
    tags = "+".join(k for k, v in (("velocity-decay", decay), ("tick-flip", flip), ("opposing-pressure", opp_press),
                                   ("stall", stall)) if v)
    # 1. profit capture (net of cost)
    if net >= min_net:
        if net >= target * cfg.mb_full_target_mult:
            return f"profit capture: +{net:.4g} >= {cfg.mb_full_target_mult:g}x target"
        if net >= target and (decay or flip or opp_press or stall):
            return f"profit capture: +{net:.4g} (target {target:.4g}) {tags}"
        if peak >= 0.5 * target and net <= peak * (1.0 - cfg.mb_giveback):
            return f"profit give-back: {net:+.4g} from peak {peak:+.4g}"
        if flip and opp_press:
            return f"momentum exit in profit: +{net:.4g} {tags}"
    # 2. micro-reversal / price failure (any P/L)
    if m.failed == -d:
        return f"failed breakout against the position ({net:+.4g})"
    if m.struct_break == -d:
        return f"micro structure break against the position ({net:+.4g})"
    if m.rejection == -d and flip:
        return f"wick rejection + tick flip ({net:+.4g})"
    if opp >= thr:
        return f"micro-reversal: opposite strength {opp:.2f} >= {thr:.2f} ({net:+.4g})"
    # 3. momentum loss while not in profit: no follow-through / momentum against
    ft = cfg.mb_follow_seconds * {"fast": 0.6, "normal": 1.0, "quiet": 1.5, "chop": 0.8}[m.regime]
    if held >= ft and peak < min_net and (vel <= 0 or opp_press):
        return f"no follow-through in {held:.1f}s (best {peak:+.4g}, now {net:+.4g})"
    if stop_dist > 0 and net <= -cfg.mb_cut_frac * stop_dist and (flip or opp_press):
        return f"momentum against: {net:+.4g} (>= {cfg.mb_cut_frac:g} of stop) {tags}"
    if cfg.mb_max_hold_seconds > 0 and held >= cfg.mb_max_hold_seconds and net < min_net:
        return f"stale: {held:.0f}s without net profit ({net:+.4g})"
    return None
