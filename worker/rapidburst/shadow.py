"""t50u SHADOW LAB: measure which short-term signal is actually right, on the owner's own feed, WITHOUT trading.

Once per M1 bar, `confirm_s` seconds after the bar opened (not on the first tick: it is noisy and the fill is worse), every rule
looks at the same tape and, if it has an opinion, records a virtual entry at the real ask (BUY) / bid (SELL). That entry then
follows the real quotes and is scored NET of the spread (exit side = bid for a BUY, ask for a SELL):

  * the move after 1 / 3 / 5 minutes, in ATR(M1) units (win rate + mean),
  * a take-profit LADDER: `levels` equal legs, leg k takes profit at k * step ATR, one shared stop `sl_atr` ATR behind the entry,
    everything still open leaves after 3 minutes. Per-leg hit rate, stop rate and the mean result per leg (the "ladder expectancy").

Rules (each says BUY / SELL / nothing):
  wick   last closed bar left a long lower (upper) wick and closed in the upper (lower) part      -> BUY (SELL)
  flow   up-vs-down tick imbalance, 5 s tape efficiency and tick run direction all agree
  mom    velocity and acceleration agree (short-term momentum)
  loc    price sits at the bottom (top) of the recent swing range with a rejection wick
  combo  at least 3 of the 4 above agree and none points the other way
  ctrl   CONTROL: alternating BUY / SELL by bar number - no information. What the other rules must beat (it shows the cost drag).

Nothing here sends an order. The numbers are SAMPLE statistics from a short window: `verdict` stays "collecting" below
`min_n` observations and only says "positive" when the mean is > 0 with z >= 2 - it is evidence, never a guarantee.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from dataclasses import dataclass, field

RULES = ("wick", "flow", "mom", "loc", "combo", "ctrl")
HORIZONS = (1, 3, 5)                    # bars (minutes) after the entry
LADDER_WINDOW_S = 180.0                 # what is still open when the ladder window ends leaves at that minute's price
MAX_LEVELS = 10
STALE_S = 25.0                          # no quote for this long: the observation is dropped, never scored on a gap


def _sgn(x: float, lim: float = 0.0) -> int:
    return 1 if x > lim else -1 if x < -lim else 0


@dataclass
class Obs:
    sym: str
    rule: str
    d: int
    t0: float
    entry: float
    atr: float
    spread: float
    last_t: float = 0.0
    best: float = 0.0                   # best net excursion so far (ATR, exit-side price)
    worst: float = 0.0
    hit: dict = field(default_factory=dict)      # leg k -> time its target was reached (before the stop)
    stop_t: float = 0.0
    marks: dict = field(default_factory=dict)    # horizon (bars) -> net result (ATR)
    ladder_closed: bool = False


@dataclass
class Stat:
    n: int = 0
    longs: int = 0
    spread_atr: float = 0.0
    sum: dict = field(default_factory=lambda: {h: 0.0 for h in HORIZONS})
    sq: dict = field(default_factory=lambda: {h: 0.0 for h in HORIZONS})
    win: dict = field(default_factory=lambda: {h: 0 for h in HORIZONS})
    hit: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    stops: int = 0
    lad_sum: float = 0.0
    lad_sq: float = 0.0

    def to_dict(self) -> dict:
        return {"n": self.n, "longs": self.longs, "spread_atr": self.spread_atr,
                "sum": {str(k): v for k, v in self.sum.items()}, "sq": {str(k): v for k, v in self.sq.items()},
                "win": {str(k): v for k, v in self.win.items()}, "hit": list(self.hit), "stops": self.stops,
                "lad_sum": self.lad_sum, "lad_sq": self.lad_sq}

    @staticmethod
    def from_dict(d: dict) -> "Stat":
        s = Stat()
        s.n, s.longs, s.spread_atr = int(d.get("n", 0)), int(d.get("longs", 0)), float(d.get("spread_atr", 0.0))
        for h in HORIZONS:
            s.sum[h] = float((d.get("sum") or {}).get(str(h), 0.0))
            s.sq[h] = float((d.get("sq") or {}).get(str(h), 0.0))
            s.win[h] = int((d.get("win") or {}).get(str(h), 0))
        hit = list(d.get("hit") or [])
        s.hit = (hit + [0] * MAX_LEVELS)[:MAX_LEVELS]
        s.stops, s.lad_sum, s.lad_sq = int(d.get("stops", 0)), float(d.get("lad_sum", 0.0)), float(d.get("lad_sq", 0.0))
        return s


def evaluate(m, wick_min: float = 0.35) -> dict:
    """The side (+1 BUY / -1 SELL / 0 none) every rule proposes for this tape. `m` is a microburst.Micro (ready)."""
    out = {r: 0 for r in RULES}
    v = getattr(m, "mtf", None)
    v1 = v.m1 if v is not None and getattr(v.m1, "ready", False) else None
    if v1 is None or m.atr <= 0:
        return out
    wick = 0
    if v1.prev_range_x >= 0.6:                       # not a dead bar
        if v1.prev_lo_wick >= wick_min and v1.prev_close_pos >= 0.55:
            wick = 1
        elif v1.prev_up_wick >= wick_min and v1.prev_close_pos <= 0.45:
            wick = -1
    flow = 0
    if abs(m.imbalance) >= 0.25 and _sgn(m.imbalance) == _sgn(m.eff, 0.05) == _sgn(m.seq_consist, 0.15):
        flow = _sgn(m.imbalance)
    mom = 0
    if abs(m.vel) >= 0.10 and _sgn(m.vel) == _sgn(m.accel, 0.0):
        mom = _sgn(m.vel)
    loc = 0
    if m.chase_up <= 0.40 and m.chase_dn >= 1.0 and m.rejection >= 0 and v1.prev_lo_wick >= 0.2:
        loc = 1
    elif m.chase_dn <= 0.40 and m.chase_up >= 1.0 and m.rejection <= 0 and v1.prev_up_wick >= 0.2:
        loc = -1
    out.update(wick=wick, flow=flow, mom=mom, loc=loc)
    votes = [wick, flow, mom, loc]
    up, dn = votes.count(1), votes.count(-1)
    out["combo"] = 1 if (up >= 3 and dn == 0) else -1 if (dn >= 3 and up == 0) else 0
    return out


def _lad_result(o: Obs, levels: int, step: float, sl: float) -> float:
    """Mean result per leg (ATR, net of the spread) of the take-profit ladder for one observation."""
    m3 = o.marks.get(3, 0.0)
    tot = 0.0
    for k in range(1, levels + 1):
        if k in o.hit:
            tot += k * step
        elif o.stop_t:
            tot += -sl
        else:
            tot += m3                                  # the window ended: it leaves at the 3-minute price
    return tot / levels


class ShadowLab:
    def __init__(self, path: str = "", step_atr: float = 0.15, levels: int = 5, sl_atr: float = 1.0):
        self.path = path
        self.step, self.levels, self.sl = step_atr, max(1, min(MAX_LEVELS, int(levels))), sl_atr
        self.pending: dict[str, list] = {}
        self.stats: dict[str, Stat] = {}              # key "rule|SYMBOL" and "rule|ALL"
        self.bar_seen: dict[str, int] = {}
        self.sig: dict[str, dict] = {}                # symbol -> {"bar", "t", "rules", "atr", "spread"} of the latest confirm point
        self.fired: dict[str, int] = {}               # (symbol|rule) -> bar a live consumer already took
        self.skipped_cost = 0
        self.dropped_stale = 0
        self.last_save = 0.0
        self.since = 0.0
        self.load()

    # ------------------------------------------------------------ feeding
    def configure(self, step_atr: float, levels: int, sl_atr: float) -> None:
        self.step, self.levels, self.sl = step_atr, max(1, min(MAX_LEVELS, int(levels))), sl_atr

    def feed(self, sym: str, m, now: float, cfg) -> None:
        """Call once per loop per symbol with the fresh Micro. Updates the open observations and, once per bar at the confirm
        point, opens new ones. Never raises into the trading loop's decisions (the caller wraps it)."""
        if not m.ready or m.atr <= 0 or m.bid <= 0 or m.ask <= 0:
            return
        self._update(sym, m, now)
        v = getattr(m, "mtf", None)
        v1 = v.m1 if v is not None and getattr(v.m1, "ready", False) else None
        if v1 is None:
            return
        age = float(v1.bar_age)
        bar = int((now - age) // 1)
        if age < cfg.shadow_confirm_s or age > cfg.shadow_confirm_s + 20.0 or self.bar_seen.get(sym) == bar:
            return
        self.bar_seen[sym] = bar
        if not self.since:
            self.since = now
        sides = evaluate(m, cfg.shadow_wick)
        sides["ctrl"] = 1 if (bar // 60) % 2 else -1
        self.sig[sym] = {"bar": bar, "t": now, "rules": dict(sides), "atr": m.atr, "spread": m.spread}
        if m.spread > cfg.shadow_max_spread_atr * m.atr:
            self.skipped_cost += 1                  # the cost alone would eat the target: not an observation
            return
        for rule, d in sides.items():
            if d:
                entry = m.ask if d > 0 else m.bid
                self.pending.setdefault(sym, []).append(Obs(sym, rule, d, now, entry, m.atr, m.spread, last_t=now))

    def _update(self, sym: str, m, now: float) -> None:
        keep = []
        for o in self.pending.get(sym, []):
            if now - o.last_t > STALE_S:
                self.dropped_stale += 1
                continue
            o.last_t = now
            px = m.bid if o.d > 0 else m.ask
            fav = o.d * (px - o.entry) / o.atr
            o.best, o.worst = max(o.best, fav), min(o.worst, fav)
            if not o.stop_t and o.worst <= -self.sl:
                o.stop_t = now                       # conservative: a stop touched in the same loop as a target counts first
            elif not o.stop_t:
                for k in range(1, self.levels + 1):
                    if k not in o.hit and o.best >= k * self.step:
                        o.hit[k] = now
            age = now - o.t0
            for h in HORIZONS:
                if h not in o.marks and age >= 60.0 * h:
                    o.marks[h] = fav
            if 3 not in o.marks and age >= LADDER_WINDOW_S:
                o.marks[3] = fav
            if 5 in o.marks:
                self._book(o)
                continue
            keep.append(o)
        self.pending[sym] = keep

    def _book(self, o: Obs) -> None:
        lad = _lad_result(o, self.levels, self.step, self.sl)
        for key in (f"{o.rule}|{o.sym}", f"{o.rule}|ALL"):
            s = self.stats.setdefault(key, Stat())
            s.n += 1
            s.longs += 1 if o.d > 0 else 0
            s.spread_atr += o.spread / o.atr
            for h in HORIZONS:
                x = o.marks.get(h, 0.0)
                s.sum[h] += x
                s.sq[h] += x * x
                s.win[h] += 1 if x > 0 else 0
            for k in o.hit:
                s.hit[k - 1] += 1
            s.stops += 1 if o.stop_t else 0
            s.lad_sum += lad
            s.lad_sq += lad * lad

    # ------------------------------------------------------------ live consumer (the TP ladder)
    def fire(self, sym: str, rule: str, bar_gap_s: float = 90.0, now: float = 0.0):
        """The side a live consumer may trade on the latest confirm point, ONCE per bar (None when the rule has no opinion,
        the bar was already taken, or the confirm point is older than bar_gap_s)."""
        sg = self.sig.get(sym)
        if not sg:
            return None
        d = int(sg["rules"].get(rule, 0))
        if not d or self.fired.get(f"{sym}|{rule}") == sg["bar"]:
            return None
        if now and now - sg["t"] > bar_gap_s:
            return None
        self.fired[f"{sym}|{rule}"] = sg["bar"]
        return {"d": d, "bar": sg["bar"], "atr": sg["atr"], "spread": sg["spread"], "rule": rule}

    # ------------------------------------------------------------ results
    @staticmethod
    def _z(sm: float, sq: float, n: int) -> float:
        if n < 3:
            return 0.0
        mean = sm / n
        var = max(sq / n - mean * mean, 0.0)
        return mean / (math.sqrt(var / n) + 1e-12) if var > 0 else 0.0

    def row(self, key: str, min_n: int) -> dict:
        s = self.stats[key]
        rule, sym = key.split("|")
        n = max(s.n, 1)
        lad = s.lad_sum / n
        z_l, z3 = self._z(s.lad_sum, s.lad_sq, s.n), self._z(s.sum[3], s.sq[3], s.n)
        if s.n < min_n:
            verdict = "collecting"
        elif lad > 0 and z_l >= 2.0:
            verdict = "positive"
        elif lad < 0 and z_l <= -2.0:
            verdict = "negative"
        else:
            verdict = "no edge"
        return {"rule": rule, "sym": sym, "n": s.n, "long_pct": round(100 * s.longs / n),
                "cost_atr": round(s.spread_atr / n, 3),
                "win": {str(h): round(100 * s.win[h] / n, 1) for h in HORIZONS},
                "avg": {str(h): round(s.sum[h] / n, 3) for h in HORIZONS},
                "hit": [round(100 * s.hit[k] / n, 1) for k in range(self.levels)],
                "stop_pct": round(100 * s.stops / n, 1), "ladder": round(lad, 3), "z_ladder": round(z_l, 2),
                "z_avg3": round(z3, 2), "verdict": verdict}

    def evidence(self, sym: str, rule: str, min_n: int) -> tuple:
        """(ok, why) for a live consumer: this rule on this symbol (or on all symbols) must be 'positive' on the lab's numbers."""
        for key in (f"{rule}|{sym}", f"{rule}|ALL"):
            if key in self.stats:
                r = self.row(key, min_n)
                if r["verdict"] == "positive":
                    return True, f"lab: {rule} on {key.split('|')[1]} n={r['n']} ladder {r['ladder']:+.3f} ATR/leg z={r['z_ladder']}"
        return False, f"lab has no positive evidence for '{rule}' yet (needs n>={min_n}, ladder > 0, z>=2)"

    def view(self, min_n: int = 100) -> dict:
        rows = [self.row(k, min_n) for k in sorted(self.stats)]
        rows.sort(key=lambda r: (r["sym"] != "ALL", r["sym"], RULES.index(r["rule"]) if r["rule"] in RULES else 99))
        return {"since": self.since or None, "step_atr": self.step, "levels": self.levels, "sl_atr": self.sl,
                "min_n": min_n, "pending": sum(len(v) for v in self.pending.values()),
                "skipped_cost": self.skipped_cost, "dropped_stale": self.dropped_stale, "rows": rows,
                "note": "virtual entries at the real ask/bid, scored net of the spread; no order is ever sent by the lab"}

    # ------------------------------------------------------------ persistence (survives a redeploy)
    def save(self, now: float = 0.0, force: bool = False) -> None:
        if not self.path or (not force and now - self.last_save < 120.0):
            return
        self.last_save = now
        try:
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path) or ".", suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"since": self.since, "stats": {k: v.to_dict() for k, v in self.stats.items()},
                           "skipped_cost": self.skipped_cost, "levels": self.levels, "step": self.step, "sl": self.sl}, fh)
            os.replace(tmp, self.path)
        except OSError:
            pass

    def load(self) -> None:
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                d = json.load(fh)
            # statistics measured with other ladder settings are not comparable: start fresh when they changed
            if (abs(float(d.get("step", self.step)) - self.step) < 1e-9 and int(d.get("levels", self.levels)) == self.levels
                    and abs(float(d.get("sl", self.sl)) - self.sl) < 1e-9):
                self.stats = {k: Stat.from_dict(v) for k, v in (d.get("stats") or {}).items()}
                self.since = float(d.get("since") or 0.0)
                self.skipped_cost = int(d.get("skipped_cost", 0))
        except (OSError, ValueError, TypeError):
            self.stats = {}
