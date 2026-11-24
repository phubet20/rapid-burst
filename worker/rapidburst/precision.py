"""t45u STRUCTURE-TO-LIQUIDITY PRECISION ENTRY (pure logic, no I/O; part of the existing engine, not a new engine).

Replaces momentum-chasing with a pullback entry. Everything is computed from the REAL broker candles the engine
already holds (M5 = structure, M1 = timing through the Micro snapshot):

  1. STRUCTURE  - confirmed swing highs/lows (fractals), BOS (close beyond the last swing in the trend direction),
                  CHOCH (close beyond the last swing AGAINST the trend = the market changes character), and the
                  DISPLACEMENT leg that made the break (origin swing -> impulse extreme, big range + big body).
  2. ORDER BLOCK - the last opposite candle before the displacement thrust (bullish OB = last down candle before the
                  up-thrust); valid until a candle closes through it.
  3. LIQUIDITY  - swing liquidity, equal highs / lows, range (previous) high / low, internal vs external, liquidity
                  SWEEPS (wick through a level, close back inside). The TARGET LIQ for a BUY is the first liquidity
                  above that leaves enough room, for a SELL the first below.
  4. FIBO       - 0.50 / 0.618 / 0.705 / 0.786 of the displacement leg, used ONLY as one confluence factor: an entry
                  needs >= `pe_min_confluence` factors (fibo, OB, S/R, sweep, FVG/demand-supply zone) AND at least one
                  that is not the fibo.
  5. TRIGGER    - rejection wick, micro structure change, or momentum re-activation on the live M1 (no bar-close wait).
  6. STOP / R:R - the stop is the STRUCTURAL INVALIDATION (beyond the pullback low / OB, never a fixed distance); the
                  entry is refused when the stop would be too wide (it is never widened), when the reward to the Target
                  LIQ is below `pe_rr_min` x the risk, when price already sits at the target, or when the spread eats
                  too much of the risk.

The result is the POSITION ENTRY SCORE (`Plan.score`), kept separate from the directional momentum score of
microburst.side_strength: right direction at a bad location is NOT an entry. Nothing here is tuned on real trades -
the numbers are defaults to be validated on a demo forward test (see README t45u).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

FIBS = (0.5, 0.618, 0.705, 0.786)


def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def _atr(bars: list, n: int = 14) -> float:
    rs = [b["high"] - b["low"] for b in bars[-n:]]
    return sum(rs) / len(rs) if rs else 0.0


def pivots(bars: list, k: int = 2) -> list:
    """Confirmed fractal swings: (index, price, 'H'|'L'). A pivot is confirmed k bars after it formed."""
    out = []
    n = len(bars)
    for i in range(k, n - k):
        h, lo = bars[i]["high"], bars[i]["low"]
        if all(h > bars[j]["high"] for j in range(i - k, i)) and all(h >= bars[j]["high"] for j in range(i + 1, i + k + 1)):
            out.append((i, h, "H"))
        if all(lo < bars[j]["low"] for j in range(i - k, i)) and all(lo <= bars[j]["low"] for j in range(i + 1, i + k + 1)):
            out.append((i, lo, "L"))
    return out


@dataclass
class OB:
    lo: float
    hi: float
    idx: int
    valid: bool = True
    touches: int = 0


@dataclass
class Event:
    """The latest structure break: BOS (with the trend) or CHOCH (against it)."""
    idx: int
    dir: int                    # +1 = broke a swing high, -1 = broke a swing low
    kind: str                   # "BOS" | "CHOCH"
    level: float                # the swing that was broken
    origin_idx: int
    origin_px: float            # the swing the impulse started from (the HL for an up break / the LH for a down break)
    t: float = 0.0              # bar time of the break (setup identity)
    body_frac: float = 0.0      # break candle: body / range (a wick spike has a small body)
    beyond: float = 0.0         # break candle close beyond the broken level, in ATR
    range_atr: float = 0.0      # break candle range in ATR (range expansion)


@dataclass
class Core:
    """Everything structural from the CLOSED bars (cached per closed M5 bar)."""
    ready: bool = False
    why: str = "warming up"
    atr: float = 0.0
    event: Optional[Event] = None
    disp_ok: bool = False
    extreme: float = 0.0        # impulse extreme: highest high (up) / lowest low (down) since the origin
    extreme_idx: int = 0
    pull_ext: float = 0.0       # deepest pullback price since the extreme (lowest low for up / highest high for down)
    ob: Optional[OB] = None
    liq: list = field(default_factory=list)       # [(price, kind, internal?)] sorted by price
    sweeps: list = field(default_factory=list)    # [(dir, level, idx)] dir +1 = lows swept (bullish)
    swing_lo: list = field(default_factory=list)  # support pivots
    swing_hi: list = field(default_factory=list)  # resistance pivots
    rng_hi: float = 0.0         # M5 range high / low of the loaded history (higher-frame context)
    rng_lo: float = 0.0
    bo_pull: float = 0.0        # counter-move extreme since the break candle (lowest low for up / highest high for down)
    n: int = 0
    last_time: float = 0.0


def analyze_core(bars: list, cfg) -> Core:
    c = Core()
    k = int(getattr(cfg, "pe_pivot_k", 2) or 2)
    need = int(getattr(cfg, "pe_min_m5_bars", 40) or 40)
    n = len(bars)
    c.n = n
    if n < need:
        c.why = f"structure warming up ({n}/{need} M5 bars)"
        return c
    atr = _atr(bars)
    if atr <= 0:
        c.why = "no volatility on M5"
        return c
    c.atr, c.last_time = atr, float(bars[-1]["time"])
    piv = pivots(bars, k)
    # ---- structure walk: BOS / CHOCH from CLOSES beyond the last confirmed swing
    trend, last_h, last_l, ev, pi = 0, None, None, None, 0
    for i in range(n):
        while pi < len(piv) and piv[pi][0] + k <= i:
            _, px, kind = piv[pi]
            if kind == "H":
                last_h = (piv[pi][0], px)
            else:
                last_l = (piv[pi][0], px)
            pi += 1
        close = bars[i]["close"]
        if last_h is not None and close > last_h[1] and last_l is not None:
            ev = Event(i, 1, "BOS" if trend > 0 else "CHOCH", last_h[1], last_l[0], last_l[1], float(bars[i]["time"]))
            trend, last_h = 1, None
        elif last_l is not None and close < last_l[1] and last_h is not None:
            ev = Event(i, -1, "BOS" if trend < 0 else "CHOCH", last_l[1], last_h[0], last_h[1], float(bars[i]["time"]))
            trend, last_l = -1, None
    c.swing_hi = [(i, p) for i, p, kd in piv if kd == "H"]
    c.swing_lo = [(i, p) for i, p, kd in piv if kd == "L"]
    if ev is None:
        c.why = "no break of structure yet (range)"
        return c
    c.event = ev
    d = ev.dir
    bb = bars[ev.idx]
    rg = max(bb["high"] - bb["low"], 1e-12)
    ev.body_frac, ev.beyond, ev.range_atr = abs(bb["close"] - bb["open"]) / rg, (bb["close"] - ev.level) * d / atr, rg / atr
    aft = bars[ev.idx + 1:]
    c.bo_pull = (min(b["low"] for b in aft) if d > 0 else max(b["high"] for b in aft)) if aft else bb["close"]
    # ---- displacement leg: origin -> extreme after the break
    seg = bars[ev.origin_idx:]
    if d > 0:
        e_i = max(range(len(seg)), key=lambda j: seg[j]["high"])
        c.extreme, c.extreme_idx = seg[e_i]["high"], ev.origin_idx + e_i
        after = bars[c.extreme_idx:]
        c.pull_ext = min(b["low"] for b in after)
        leg = c.extreme - ev.origin_px
    else:
        e_i = min(range(len(seg)), key=lambda j: seg[j]["low"])
        c.extreme, c.extreme_idx = seg[e_i]["low"], ev.origin_idx + e_i
        after = bars[c.extreme_idx:]
        c.pull_ext = max(b["high"] for b in after)
        leg = ev.origin_px - c.extreme
    body = max(abs(b["close"] - b["open"]) for b in bars[ev.origin_idx:c.extreme_idx + 1]) if c.extreme_idx >= ev.origin_idx else 0.0
    c.disp_ok = leg >= float(getattr(cfg, "pe_disp_atr", 1.5)) * atr and body >= float(getattr(cfg, "pe_disp_body_atr", 0.8)) * atr
    # ---- order block: the last opposite candle before the thrust candle (largest body in the leg)
    thr = bars[ev.origin_idx:c.extreme_idx + 1]
    if thr:
        sgn = (lambda b: b["close"] - b["open"]) if d > 0 else (lambda b: b["open"] - b["close"])
        j = max(range(len(thr)), key=lambda x: sgn(thr[x]))
        cand = [x for x in range(j - 1, -1, -1) if sgn(thr[x]) < 0]
        ob_i = ev.origin_idx + (cand[0] if cand else 0)
        ob = bars[ob_i]
        if d > 0:
            o = OB(ob["low"], max(ob["open"], ob["close"]), ob_i)
        else:
            o = OB(min(ob["open"], ob["close"]), ob["high"], ob_i)
        for j2 in range(ob_i + 1, n):
            b = bars[j2]
            if (d > 0 and b["close"] < o.lo) or (d < 0 and b["close"] > o.hi):
                o.valid = False
            if j2 > c.extreme_idx and b["low"] <= o.hi and b["high"] >= o.lo:
                o.touches += 1
        c.ob = o
    # ---- liquidity map: swing liquidity, equal highs / lows, range extremes, sweeps
    tol = float(getattr(cfg, "pe_liq_tol_atr", 0.12)) * atr
    liq = []
    hi_all = max(b["high"] for b in bars)
    lo_all = min(b["low"] for b in bars)
    c.rng_hi, c.rng_lo = hi_all, lo_all
    for _, p in c.swing_hi:
        liq.append((p, "swing high", p < hi_all - 1e-12))
    for _, p in c.swing_lo:
        liq.append((p, "swing low", p > lo_all + 1e-12))
    for lst, kind in ((c.swing_hi, "equal highs"), (c.swing_lo, "equal lows")):
        for a in range(len(lst)):
            for b in range(a + 1, len(lst)):
                if abs(lst[a][1] - lst[b][1]) <= tol:
                    liq.append(((lst[a][1] + lst[b][1]) / 2.0, kind, True))
    liq.append((hi_all, "range high (previous high)", False))
    liq.append((lo_all, "range low (previous low)", False))
    c.liq = sorted(liq, key=lambda x: x[0])
    sweeps = []
    for i in range(max(1, n - 10), n):
        b = bars[i]
        for pi_, p in c.swing_lo:
            if pi_ < i - 1 and b["low"] < p - 0.03 * atr and b["close"] > p:
                sweeps.append((1, p, i))
        for pi_, p in c.swing_hi:
            if pi_ < i - 1 and b["high"] > p + 0.03 * atr and b["close"] < p:
                sweeps.append((-1, p, i))
    c.sweeps = sorted(set(sweeps))
    c.ready = True
    c.why = "ok"
    return c


@dataclass
class PrecisionView:
    core: Core = field(default_factory=Core)
    live_hi: float = 0.0
    live_lo: float = 0.0

    @property
    def ready(self) -> bool:
        return self.core.ready

    def brief(self) -> dict:
        c = self.core
        if not c.ready:
            return {"ready": False, "why": c.why}
        ev = c.event
        return {"ready": True, "dir": ev.dir, "event": ev.kind, "displacement": c.disp_ok,
                "origin": round(ev.origin_px, 5), "extreme": round(c.extreme, 5),
                "ob": None if c.ob is None else [round(c.ob.lo, 5), round(c.ob.hi, 5), c.ob.valid, c.ob.touches],
                "liq": [(round(p, 5), k) for p, k, _ in c.liq][-12:], "sweeps": len(c.sweeps)}


class Analyzer:
    """Caches the structural core per closed M5 bar (one pass over <= 300 bars per new bar, not per tick)."""

    def __init__(self):
        self._c: dict = {}

    def view(self, key: str, bars: list, live: Optional[dict], cfg) -> PrecisionView:
        stamp = (len(bars), float(bars[-1]["time"]) if bars else 0.0)
        hit = self._c.get(key)
        if hit is None or hit[0] != stamp:
            hit = (stamp, analyze_core(list(bars), cfg))
            self._c[key] = hit
        lh = float(live["high"]) if live else 0.0
        ll = float(live["low"]) if live else 0.0
        return PrecisionView(hit[1], lh, ll)


@dataclass
class Plan:
    side: str
    ready: bool = False
    score: float = 0.0
    vetoes: list = field(default_factory=list)
    tags: list = field(default_factory=list)
    comps: dict = field(default_factory=dict)
    key: str = ""                 # setup identity: one entry per structure event
    kind: str = ""                # BOS | CHOCH
    entry: float = 0.0
    sl: float = 0.0               # structural invalidation (price)
    inval: float = 0.0            # the level the engine exits at (before the buffer)
    target: float = 0.0
    target_kind: str = ""
    rr: float = 0.0
    risk: float = 0.0
    reward: float = 0.0
    depth: float = 0.0
    zone: tuple = (0.0, 0.0)
    origin: float = 0.0
    extreme: float = 0.0
    waiting: str = ""             # what the engine is waiting for
    path: str = "pullback"        # "pullback" | "breakout" (confirmed breakout continuation / retest)
    prem: float = 0.0             # 0 = deepest discount for this side ... 1 = top premium of the M5 range

    def ok(self, cfg) -> bool:
        return self.ready and not self.vetoes and self.score >= float(getattr(cfg, "pe_min", 0.58))

    def brief(self) -> dict:
        r = lambda x: round(x, 5)    # noqa: E731
        return {"side": self.side, "pe": round(self.score, 3), "ok": not self.vetoes, "vetoes": self.vetoes[:3],
                "tags": self.tags, "rr": round(self.rr, 2), "sl": r(self.sl), "target": r(self.target),
                "target_kind": self.target_kind, "depth": round(self.depth, 3), "waiting": self.waiting,
                "path": self.path, "premium": round(self.prem, 2),
                "comps": {k: round(v, 2) for k, v in self.comps.items()}}


W = {"structure": 0.14, "ob": 0.12, "liquidity": 0.08, "fibo": 0.12, "sr": 0.06, "pullback": 0.10, "rejection": 0.08,
     "reactivation": 0.10, "volume": 0.04, "space": 0.08, "rr": 0.10, "cost": 0.04, "location": 0.06}
# confirmed-breakout path: the zone factors (OB / fibo / S-R) do not exist there; the breakout quality replaces them
W_BO = {"structure": 0.14, "breakout": 0.30, "liquidity": 0.08, "pullback": 0.06, "rejection": 0.04,
        "reactivation": 0.12, "volume": 0.06, "space": 0.08, "rr": 0.10, "cost": 0.04}


def _breakout(c: Core, ev: Event, m, d: int, cfg, view: PrecisionView, mid: float, atr: float):
    """Confirmed breakout continuation / breakout retest. Returns (info | None, why). Not every spike: the break candle
    must be a real body that CLOSED beyond the level with range expansion, the break must be fresh, price must not have
    run far beyond the broken level, and momentum must be pushing in the trade direction right now."""
    if not getattr(cfg, "pe_breakout_enabled", True):
        return None, "breakout entries are disabled"
    nb = c.n - 1 - ev.idx
    if nb > int(getattr(cfg, "pe_bo_max_bars", 8)):
        return None, f"the break is {nb} M5 bars old - no fresh breakout to continue"
    if ev.body_frac < float(getattr(cfg, "pe_bo_body_frac", 0.5)) or ev.beyond < float(getattr(cfg, "pe_bo_beyond_atr", 0.1)) \
            or ev.range_atr < float(getattr(cfg, "pe_bo_range_atr", 1.0)):
        return None, (f"the break candle looks like a spike (body {ev.body_frac:.0%} of range, closed {ev.beyond:.2f} ATR beyond, "
                      f"range {ev.range_atr:.2f} ATR) - not a confirmed breakout")
    bp = c.bo_pull
    if d > 0 and view.live_lo:
        bp = min(bp, view.live_lo)
    elif d < 0 and view.live_hi:
        bp = max(bp, view.live_hi)
    back = (bp - ev.level) * d / atr                      # how far (ATR) the counter-move went relative to the broken level
    if back < -0.25:
        return None, "price fell back through the broken level - the breakout failed"
    retest = back <= 0.35
    ext = (mid - ev.level) * d / atr
    lim = float(getattr(cfg, "pe_bo_retest_ext_atr", 1.5)) if retest else float(getattr(cfg, "pe_bo_cont_ext_atr", 0.8))
    if ext > lim:
        return None, f"price is {ext:.2f} ATR beyond the broken level (max {lim:g}) - chasing the breakout"
    if not (d * m.vel > 0 and d * m.accel >= 0):
        return None, "waiting for momentum to push in the breakout direction"
    q = _clip(0.4 * _clip((ev.body_frac - 0.3) / 0.5) + 0.2 * _clip(ev.beyond / 0.5) + 0.2 * _clip(ev.range_atr / 1.5)
              + 0.2 * (1.0 if retest else 0.0))
    inval = min(ev.level, bp) if d > 0 else max(ev.level, bp)
    return {"retest": retest, "q": q, "inval": inval}, ""


def evaluate(view: PrecisionView, m, d: int, cfg) -> Plan:
    """Position Entry Score + structural stop + Target LIQ for one side, from the current Micro snapshot `m`."""
    side = "BUY" if d > 0 else "SELL"
    p = Plan(side)
    c = view.core
    if not c.ready:
        p.vetoes.append(c.why)
        return p
    ev = c.event
    atr = c.atr
    ref = m.ask if d > 0 else m.bid
    mid = m.mid
    if atr <= 0 or not ref or not mid:
        p.vetoes.append("no price / volatility")
        return p
    p.kind = ev.kind
    p.key = f"{ev.t:.0f}:{ev.dir}"
    if ev.dir != d:
        p.vetoes.append(f"M5 structure is {'UP' if ev.dir > 0 else 'DOWN'} ({ev.kind} {'up' if ev.dir > 0 else 'down'}) - "
                        f"no {side} against it")
        p.waiting = "a break of structure in the trade direction"
        return p
    if not c.disp_ok:
        p.vetoes.append("the break has no displacement (small range / small bodies) - no impulse to pull back from")
        return p
    spread = float(m.spread or 0.0)
    # ---- the displacement leg, with the live forming bar included
    if d > 0:
        ext = max(c.extreme, view.live_hi or 0.0, mid)
        origin = ev.origin_px
        leg = ext - origin
        pull = min(c.pull_ext, mid) if ext == c.extreme else mid
        if view.live_lo and ext == c.extreme:
            pull = min(pull, view.live_lo)
        depth = (ext - mid) / max(leg, 1e-12)
        deepest = (ext - pull) / max(leg, 1e-12)
        broken = pull <= origin
    else:
        ext = min(c.extreme, view.live_lo or 1e30, mid)
        origin = ev.origin_px
        leg = origin - ext
        pull = max(c.pull_ext, mid) if ext == c.extreme else mid
        if view.live_hi and ext == c.extreme:
            pull = max(pull, view.live_hi)
        depth = (mid - ext) / max(leg, 1e-12)
        deepest = (pull - ext) / max(leg, 1e-12)
        broken = pull >= origin
    p.origin, p.extreme, p.depth = origin, ext, depth
    if leg < float(getattr(cfg, "pe_disp_atr", 1.5)) * atr * 0.8:
        p.vetoes.append("impulse leg too small")
        return p
    if broken or deepest > float(getattr(cfg, "pe_max_depth", 0.886)):
        p.vetoes.append(f"structure lost: pullback {deepest:.2f} of the leg (past the {getattr(cfg, 'pe_max_depth', 0.886):.3f} "
                        f"limit / the impulse origin) - the setup is invalid")
        return p

    def lvl(f: float) -> float:
        return ext - d * f * leg

    # ---- confluence factors at the pullback
    fib_lo, fib_hi = sorted((lvl(0.786), lvl(0.5)))
    tap = max(depth, deepest)
    tol_f = 0.04
    fib_hit = 0.5 - tol_f <= tap <= 0.786 + tol_f and depth <= 0.786 + tol_f + 0.15
    fib_q = 0.0
    if fib_hit:
        fib_q = 1.0 if 0.60 <= tap <= 0.72 else 0.8 if 0.5 <= tap <= 0.786 else 0.4
    ob = c.ob
    ob_hit, ob_q = False, 0.0
    if ob is not None and ob.valid:
        near = 0.15 * atr
        tapped = (pull <= ob.hi + near and pull >= ob.lo - 0.3 * atr) if d > 0 else (pull >= ob.lo - near and pull <= ob.hi + 0.3 * atr)
        ob_hit = tapped
        ob_q = (1.0 if ob.touches <= 1 else 0.6) if ob_hit else 0.0
    sr_hit = False
    levels = [pp for _, pp in (c.swing_lo if d > 0 else c.swing_hi)]
    iv = getattr(m, "ind", None)
    extra = getattr(iv, "sup_below" if d > 0 else "res_above", None) if iv is not None else None
    if extra:
        levels.append(float(extra))
    for lv in levels:
        if abs(lv - origin) < 1e-9:
            continue
        if abs(pull - lv) <= 0.3 * atr:
            sr_hit = True
            break
    sw = [s for s in c.sweeps if s[0] == d and s[2] > ev.idx]
    sweep_hit = bool(sw)
    zv = getattr(m, "zones", None)
    zone_hit = bool(zv is not None and getattr(zv, "ready", False) and zv.holds(d))
    facs = [n for n, h in (("fibo", fib_hit), ("OB", ob_hit), ("S/R", sr_hit), ("sweep", sweep_hit), ("FVG/zone", zone_hit)) if h]
    p.tags = facs
    zone_lo, zone_hi = fib_lo, fib_hi
    if ob_hit and ob is not None:
        zone_lo, zone_hi = min(zone_lo, ob.lo), max(zone_hi, ob.hi)
    p.zone = (zone_lo, zone_hi)
    # ---- location: premium / discount of the M5 range, pullback zone or a confirmed breakout (anti-chase)
    rh, rl = max(c.rng_hi, mid), min(c.rng_lo, mid)
    rpos = (mid - rl) / max(rh - rl, 1e-12)
    prem = rpos if d > 0 else 1.0 - rpos
    p.prem = prem
    top = (zone_hi if d > 0 else zone_lo)
    ran = (mid - top) if d > 0 else (top - mid)           # distance price has already travelled back out of the zone
    in_zone = not (depth < float(getattr(cfg, "pe_min_depth", 0.30)) or ran > float(getattr(cfg, "pe_zone_chase_atr", 0.35)) * atr)
    bo = None
    if in_zone:
        non_fib = [f for f in facs if f != "fibo"]
        if len(facs) < int(getattr(cfg, "pe_min_confluence", 2)) or not non_fib:
            p.vetoes.append(f"no confluence in the zone ({'+'.join(facs) or 'none'}): fibo alone is not an entry")
            p.waiting = "OB / S-R / sweep / FVG overlapping the fibo zone"
            return p
        if prem > float(getattr(cfg, "pe_range_prem_max", 0.85)):
            p.vetoes.append(f"price is in the {'premium' if d > 0 else 'discount'} extreme of the M5 range ({prem:.0%}) - "
                            f"{'buying' if d > 0 else 'selling'} into the {'highs' if d > 0 else 'lows'}")
            p.waiting = f"a pullback away from the M5 range {'high' if d > 0 else 'low'}"
            return p
    else:
        bo, why_bo = _breakout(c, ev, m, d, cfg, view, mid, atr)
        if bo is None:
            p.vetoes.append(f"price is not in the pullback zone (depth {depth:.2f}, {max(0.0, ran) / atr:.2f} ATR beyond it) - "
                            f"not chasing; no breakout alternative: {why_bo}")
            p.waiting = f"a pullback into {fib_lo:.5g}-{fib_hi:.5g}"
            return p
        p.path = "breakout"
        facs = ["breakout-retest" if bo["retest"] else "breakout"]
        p.tags = list(facs)
    # ---- trigger: rejection, micro structure change, momentum re-activation (live M1, no bar close)
    rej = (m.rejection == d) or (zv is not None and getattr(zv, "wick", 0) == d)
    micro = d in (m.struct_break, m.breakout, m.failed, m.rebound)
    react = d * m.vel > 0 and d * m.accel >= 0
    p.tags = facs + [t for t, h in (("rejection", rej), ("micro-structure", micro), ("re-activation", react)) if h]
    if bo is None and not (rej or micro or react):
        p.waiting = "a rejection wick / micro structure change / momentum re-activation"
        p.vetoes.append("in the zone, waiting for the trigger (rejection, micro structure change or re-activation)")
    # ---- structural stop (the setup is wrong if price takes this out) - never widened
    buf = max(float(getattr(cfg, "pe_sl_buf_atr", 0.12)) * atr, 2.0 * spread)
    base = pull
    if bo is not None:
        base = bo["inval"]                      # the breakout fails when price is back through the broken level
    else:
        if ob_hit and ob is not None:
            base = min(base, ob.lo) if d > 0 else max(base, ob.hi)
        if sweep_hit:
            base = min(base, min(s[1] for s in sw)) if d > 0 else max(base, max(s[1] for s in sw))
    inval = base
    sl = inval - d * buf
    risk = (ref - sl) * d
    p.entry, p.sl, p.inval, p.risk = ref, sl, inval, risk
    if risk <= 0:
        p.vetoes.append("price is already beyond the invalidation level")
        return p
    if risk > float(getattr(cfg, "pe_max_sl_atr", 1.6)) * atr:
        p.vetoes.append(f"structural stop {risk:.5g} = {risk / atr:.2f} ATR is too wide (max "
                        f"{getattr(cfg, 'pe_max_sl_atr', 1.6):g}) - the stop is not widened, the entry is skipped")
    cost_frac = spread / risk
    if cost_frac > float(getattr(cfg, "pe_max_cost_frac", 0.30)):
        p.vetoes.append(f"spread {spread:.5g} is {cost_frac:.0%} of the stop risk (max {getattr(cfg, 'pe_max_cost_frac', 0.30):.0%})")
    # ---- target LIQ with room and reward-to-risk
    cands = [(price, kind) for price, kind, _ in c.liq if (price - ref) * d > 0]
    cands.append((ext, "impulse extreme"))
    cands = sorted({round(pp, 8): (pp, kk) for pp, kk in cands if (pp - ref) * d > 0}.values(), key=lambda x: d * x[0])
    near = float(getattr(cfg, "pe_near_atr", 0.30)) * atr
    if cands and abs(cands[0][0] - ref) < near:
        p.vetoes.append(f"price is at liquidity already ({cands[0][1]} {abs(cands[0][0] - ref) / atr:.2f} ATR away) - "
                        f"no room")
    tmin = float(getattr(cfg, "pe_tgt_min_atr", 0.8)) * atr
    tmax = float(getattr(cfg, "pe_tgt_max_atr", 6.0)) * atr
    rr_min = float(getattr(cfg, "pe_rr_min", 1.5))
    tgt = None
    for rank, (price, kind) in enumerate(cands):
        dist = (price - ref) * d
        if dist < tmin:
            continue
        if dist > tmax:
            break
        rr = (dist - spread) / risk
        if rr >= rr_min:
            tgt = (price, kind, dist, rr, rank)
            break
    if tgt is None:
        p.vetoes.append(f"no liquidity target with reward >= {rr_min:g}x the risk (stop {risk:.5g}) within "
                        f"{getattr(cfg, 'pe_tgt_max_atr', 6.0):g} ATR")
        p.waiting = p.waiting or "more room to a liquidity target"
    else:
        p.target, p.target_kind, p.reward, p.rr = tgt[0], tgt[1], tgt[2], tgt[3]
    # ---- Position Entry Score (separate from the directional momentum score)
    v1 = getattr(getattr(m, "mtf", None), "m1", None)
    vol_x = getattr(v1, "vol_x", None) if v1 is not None else None
    vol = _clip(((vol_x or 1.0) - 0.9) / 0.6) if (vol_x is not None or getattr(m, "act_ratio", 1.0) > 1.0) else 0.0
    vol = max(vol, _clip((getattr(m, "act_ratio", 1.0) - 1.0)))
    comps = {
        "structure": _clip(0.55 + 0.15 * (ev.kind == "BOS") + 0.30 * _clip(leg / (2.5 * atr))),
        "ob": ob_q,
        "liquidity": _clip(0.6 * sweep_hit + 0.4 * (tgt is not None and tgt[4] == 0)),
        "fibo": fib_q,
        "sr": 1.0 if sr_hit else 0.0,
        "pullback": _clip(0.6 * (1.0 if 0.5 <= tap <= 0.786 else 0.6 if 0.382 <= tap < 0.5 else 0.3) + 0.4 * (1.0 - _clip((deepest - 0.786) / 0.1))),
        "rejection": 1.0 if rej else 0.0,
        "reactivation": 1.0 if (react and micro) else 0.7 if react else 0.4 if micro else 0.0,
        "volume": vol,
        "space": 0.0 if tgt is None else _clip((tgt[2] / atr - 0.8) / 2.0),
        "rr": 0.0 if tgt is None else _clip((tgt[3] - 1.0) / 2.0),
        "cost": _clip(1.0 - cost_frac / max(float(getattr(cfg, "pe_max_cost_frac", 0.30)), 1e-9)),
        "location": _clip(1.0 - prem),
    }
    w = W
    if bo is not None:
        for k in ("ob", "fibo", "sr", "location"):
            comps.pop(k, None)
        comps["breakout"] = bo["q"]
        comps["pullback"] = 1.0 if bo["retest"] else 0.4
        w = W_BO
    p.comps = comps
    p.score = _clip(sum(w[k] * v for k, v in comps.items()) / sum(w[k] for k in comps))
    p.ready = True
    if not p.vetoes and p.score < float(getattr(cfg, "pe_min", 0.58)):
        p.waiting = f"a better entry: position score {p.score:.2f} < {getattr(cfg, 'pe_min', 0.58):.2f}"
    return p
