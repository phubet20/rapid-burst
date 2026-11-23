"""M5 direction bias + M1 entry timing from REAL broker candles merged with the live tick stream.

* Closed bars come from the broker (TickerAll GET /candles: OHLC bid + tickVolume); the forming bar is built
  from live ticks between polls (no candle-close wait). Nothing is synthesised: when the broker sends no
  bars, only bars observed live from ticks are used and the source says so.
* EMA(8/21), MACD(12,26,9) and RSI(14, Wilder) are incremental: committed state advances once per CLOSED bar; the forming
  bar is evaluated with an O(1) "peek" on every call. A broker refresh that changes history recomputes
  at most `maxlen` bars once per poll (not per tick).
* tickVolume = number of price changes the broker counted (tick volume), NOT exchange volume. The live
  bar's activity is compared with bars measured the SAME way (own live tick counts, or broker tickVolume
  for the last closed bars) - the two sources are never mixed in one ratio.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Optional


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def _a(n: int) -> float:
    return 2.0 / (n + 1.0)


@dataclass
class _St:
    """Committed indicator state after the last closed bar."""
    n: int = 0
    e8: float = 0.0
    e21: float = 0.0
    e12: float = 0.0
    e26: float = 0.0
    sig: float = 0.0
    c: float = 0.0               # last close (RSI change reference)
    ag: float = 0.0              # Wilder average gain / loss (RSI 14)
    al: float = 0.0

    def step(self, c: float) -> "_St":
        if self.n == 0:
            return _St(1, c, c, c, c, 0.0, c, 0.0, 0.0)
        e12 = self.e12 + _a(12) * (c - self.e12)
        e26 = self.e26 + _a(26) * (c - self.e26)
        m = e12 - e26
        sig = m if self.n == 1 else self.sig + _a(9) * (m - self.sig)
        ch = c - self.c
        k = min(self.n, 14)                      # SMA seed for the first 14 changes, then Wilder smoothing
        ag = self.ag + (max(ch, 0.0) - self.ag) / k
        al = self.al + (max(-ch, 0.0) - self.al) / k
        return _St(self.n + 1, self.e8 + _a(8) * (c - self.e8), self.e21 + _a(21) * (c - self.e21), e12, e26, sig,
                   c, ag, al)

    @property
    def rsi(self) -> float:
        if self.n < 2:
            return 50.0
        if self.al <= 0:
            return 100.0 if self.ag > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + self.ag / self.al)

    @property
    def macd(self) -> float:
        return self.e12 - self.e26

    @property
    def hist(self) -> float:
        return self.macd - self.sig


@dataclass
class TFState:
    """What one timeframe says right now (forming bar included)."""
    ready: bool = False
    src: str = "none"
    bars: int = 0
    dir: int = 0                 # EMA trend direction (+1 up / -1 down / 0 flat)
    trend: float = 0.0           # -1..1 EMA slope + price vs EMA, in average-range units
    body: float = 0.0            # forming bar body / average range (signed)
    range_x: float = 0.0         # forming bar range / average range
    macd: float = 0.0            # in average-range units
    hist: float = 0.0
    hist_slope: float = 0.0      # forming hist - last closed hist (avg-range units)
    hist_accel: float = 0.0      # change of that slope vs the previous bar
    macd_ready: bool = False
    vol_x: Optional[float] = None    # forming-bar activity vs comparable previous bars (time-normalised)
    vol_src: str = "n/a"
    structure: int = 0           # +1 HH/HL, -1 LH/LL, 0 mixed (last closed bars)
    exhaust: int = 0             # +1 up move exhausting, -1 down move exhausting
    avg_range: float = 0.0
    rsi: float = 50.0            # RSI(14) with the forming bar (soft feature only - never a gate or a cross wait)
    rsi_slope: float = 0.0       # forming RSI - last closed RSI
    rsi_accel: float = 0.0       # change of that slope vs the previous bar (momentum change)
    rsi_ready: bool = False
    swing: int = 0               # +1 swing HH + HL, -1 swing LH + LL, 0 mixed (pivot highs/lows of closed bars)
    sbreak: int = 0              # +1 price is above the last swing high (break up), -1 below the last swing low
    swing_hi: float = 0.0
    swing_lo: float = 0.0

    def brief(self) -> dict:
        r = lambda x, n=2: None if x is None else round(x, n)   # noqa: E731
        return {"ready": self.ready, "src": self.src, "bars": self.bars, "dir": self.dir, "trend": r(self.trend),
                "body": r(self.body), "range_x": r(self.range_x), "macd": r(self.macd, 3), "hist": r(self.hist, 3),
                "hist_slope": r(self.hist_slope, 3), "macd_ready": self.macd_ready, "vol_x": r(self.vol_x),
                "vol_src": self.vol_src, "structure": self.structure, "exhaust": self.exhaust,
                "rsi": r(self.rsi, 1), "rsi_slope": r(self.rsi_slope, 1), "rsi_ready": self.rsi_ready,
                "swing": self.swing, "sbreak": self.sbreak}


class TF:
    def __init__(self, sec: int, maxlen: int = 300):
        self.sec = sec
        self.maxlen = maxlen
        self.bars: deque = deque(maxlen=maxlen)     # closed: {"time","open","high","low","close","vol","own"}
        self.st = _St()
        self.hists: deque = deque(maxlen=4)         # committed hist values (last closed bars)
        self.e21s: deque = deque(maxlen=4)
        self.rsis: deque = deque(maxlen=4)
        self.live: Optional[dict] = None
        self.own: dict = {}                          # bar time -> own live tick count (bars observed live)
        self.broker_ok = False

    # ------------------------------------------------------------ building
    def _commit(self, b: dict) -> None:
        self.bars.append(b)
        self.st = self.st.step(b["close"])
        self.hists.append(self.st.hist)
        self.e21s.append(self.st.e21)
        self.rsis.append(self.st.rsi)

    def _rebuild(self, closed: list) -> None:
        self.bars = deque(maxlen=self.maxlen)
        self.st = _St()
        self.hists.clear()
        self.e21s.clear()
        self.rsis.clear()
        for b in closed[-self.maxlen:]:
            self._commit(b)

    def load(self, candles: list, now: float) -> None:
        """Merge a broker poll (oldest -> newest). Recomputes only when closed history changed."""
        if not candles:
            return
        self.broker_ok = True
        closed, forming = [], None
        for c in candles:
            b = {"time": float(c["time"]), "open": float(c["open"]), "high": float(c["high"]),
                 "low": float(c["low"]), "close": float(c["close"]),
                 "vol": float(c["tick_volume"]) if c.get("tick_volume") is not None else None}
            if b["time"] + self.sec <= now:
                b["own"] = self.own.get(b["time"])
                closed.append(b)
            else:
                forming = b
        if closed:
            if not self.bars or closed[0]["time"] < self.bars[0]["time"]:
                self._rebuild(closed)                 # first poll / broker has older history than we have
            else:
                mine = {b["time"]: b for b in list(self.bars)[-6:]}
                changed = any(abs(b["close"] - mine[b["time"]]["close"]) > 1e-9
                              or abs(b["high"] - mine[b["time"]]["high"]) > 1e-9
                              or abs(b["low"] - mine[b["time"]]["low"]) > 1e-9
                              for b in closed if b["time"] in mine)
                if changed:                           # broker corrected a recent bar (e.g. one built from ticks)
                    keep = [b for b in self.bars if b["time"] < closed[0]["time"]]
                    self._rebuild(keep + closed)
                else:
                    for b in closed:
                        if b["time"] > self.bars[-1]["time"]:
                            self._commit(b)
                        elif b["time"] in mine and b["vol"] is not None:
                            mine[b["time"]]["vol"] = b["vol"]
        if forming is not None:
            lv = self.live
            if lv is None or lv["time"] < forming["time"]:
                self.live = dict(forming, own=0)
            elif lv["time"] == forming["time"]:
                lv["open"] = forming["open"]
                lv["high"] = max(lv["high"], forming["high"])
                lv["low"] = min(lv["low"], forming["low"])
                lv["vol"] = forming["vol"]

    def on_tick(self, t: float, price: float) -> None:
        bt = t - (t % self.sec)
        lv = self.live
        if lv is None or bt > lv["time"]:
            if lv is not None and (not self.bars or self.bars[-1]["time"] < lv["time"]):
                self.own[lv["time"]] = lv["own"]
                self._commit({k: lv[k] for k in ("time", "open", "high", "low", "close", "vol", "own")})
            elif lv is not None:
                self.own[lv["time"]] = lv["own"]
                if self.bars and self.bars[-1]["time"] == lv["time"]:
                    self.bars[-1]["own"] = lv["own"]
            if len(self.own) > 2 * self.maxlen:
                for k in sorted(self.own)[: len(self.own) - self.maxlen]:
                    del self.own[k]
            self.live = lv = {"time": bt, "open": price, "high": price, "low": price, "close": price,
                              "vol": None, "own": 0}
        elif bt < lv["time"]:
            return
        lv["close"] = price
        if price > lv["high"]:
            lv["high"] = price
        if price < lv["low"]:
            lv["low"] = price
        lv["own"] += 1

    # ------------------------------------------------------------ reading
    def state(self, now: float) -> TFState:
        out = TFState(src="broker+ticks" if self.broker_ok else "ticks only", bars=len(self.bars))
        lv = self.live
        if lv is None or len(self.bars) < 6:
            return out
        recent = list(self.bars)[-10:]
        ar = sum(b["high"] - b["low"] for b in recent) / len(recent)
        if ar <= 0:
            return out
        out.avg_range = ar
        out.ready = True
        p = self.st.step(lv["close"])                 # O(1) peek with the forming bar
        out.macd_ready = self.st.n >= 35
        out.macd = p.macd / ar
        out.hist = p.hist / ar
        h1 = self.hists[-1] if self.hists else 0.0
        h2 = self.hists[-2] if len(self.hists) >= 2 else h1
        out.hist_slope = (p.hist - h1) / ar
        out.hist_accel = ((p.hist - h1) - (h1 - h2)) / ar
        out.rsi_ready = self.st.n >= 15
        out.rsi = p.rsi
        r1 = self.rsis[-1] if self.rsis else p.rsi
        r2 = self.rsis[-2] if len(self.rsis) >= 2 else r1
        out.rsi_slope = p.rsi - r1
        out.rsi_accel = (p.rsi - r1) - (r1 - r2)
        slope = (p.e21 - (self.e21s[-2] if len(self.e21s) >= 2 else p.e21)) / ar
        pos = (lv["close"] - p.e21) / ar
        out.trend = _clip(0.6 * _clip(slope / 0.3) + 0.4 * _clip(pos / 1.5) + 0.3 * _clip((p.e8 - p.e21) / ar))
        out.dir = 1 if out.trend > 0.2 else -1 if out.trend < -0.2 else 0
        out.body = (lv["close"] - lv["open"]) / ar
        out.range_x = (lv["high"] - lv["low"]) / ar
        frac = max(0.25, min(1.0, (now - lv["time"]) / self.sec))
        own_prev = [b["own"] for b in recent if b.get("own")]
        vol_prev = [b["vol"] for b in recent if b.get("vol")]
        if len(own_prev) >= 3 and lv["own"]:
            out.vol_x, out.vol_src = (lv["own"] / frac) / (sum(own_prev) / len(own_prev)), "live tick count"
        elif lv.get("vol") and len(vol_prev) >= 3:
            out.vol_x, out.vol_src = (lv["vol"] / frac) / (sum(vol_prev) / len(vol_prev)), "broker tickVolume"
        elif len(vol_prev) >= 4:
            out.vol_x, out.vol_src = vol_prev[-1] / (sum(vol_prev[:-1]) / (len(vol_prev) - 1)), "broker tickVolume (last closed)"
        b3 = recent[-3:]
        if len(b3) == 3:
            hh = b3[2]["high"] > b3[1]["high"] > b3[0]["high"] or (b3[2]["high"] > b3[1]["high"] and b3[2]["low"] > b3[1]["low"])
            ll = b3[2]["low"] < b3[1]["low"] < b3[0]["low"] or (b3[2]["low"] < b3[1]["low"] and b3[2]["high"] < b3[1]["high"])
            out.structure = 1 if hh and not ll else -1 if ll and not hh else 0
        # swing structure from pivot highs/lows (1 bar each side) of the last 30 closed bars: HH/HL, LH/LL
        bars = list(self.bars)[-30:]
        ph = [bars[i]["high"] for i in range(1, len(bars) - 1)
              if bars[i]["high"] >= bars[i - 1]["high"] and bars[i]["high"] >= bars[i + 1]["high"]]
        pl = [bars[i]["low"] for i in range(1, len(bars) - 1)
              if bars[i]["low"] <= bars[i - 1]["low"] and bars[i]["low"] <= bars[i + 1]["low"]]
        if len(ph) >= 2 and len(pl) >= 2:
            up = ph[-1] > ph[-2] and pl[-1] > pl[-2]
            dn = ph[-1] < ph[-2] and pl[-1] < pl[-2]
            out.swing = 1 if up else -1 if dn else 0
        if ph:
            out.swing_hi = ph[-1]
        if pl:
            out.swing_lo = pl[-1]
        if ph and lv["close"] > ph[-1]:
            out.sbreak = 1
        elif pl and lv["close"] < pl[-1]:
            out.sbreak = -1
        ext = pos
        if ext > 1.5 and p.hist < h1 and h1 < h2:
            out.exhaust = 1
        elif ext < -1.5 and p.hist > h1 and h1 > h2:
            out.exhaust = -1
        return out


@dataclass
class MTFView:
    """M1-ONLY view (t30u): `bias` = the M1 MAIN TREND (EMA + MACD + HH/HL-LH/LL swing structure), `m1_up/dn` =
    the M1 intrabar trigger. t32u: the dead M5 alias is removed."""
    m1: TFState = field(default_factory=TFState)
    bias: float = 0.0            # -1..1 M1 main-trend bias (signed)
    bias_label: str = "unknown"
    supported: bool = False
    m1_up: float = 0.0           # M1 timing score for BUY 0..1
    m1_dn: float = 0.0

    def timing(self, d: int) -> float:
        return self.m1_up if d > 0 else self.m1_dn

    def brief(self) -> dict:
        return {"bias": round(self.bias, 2), "label": self.bias_label, "supported": self.supported,
                "m1_up": round(self.m1_up, 2), "m1_dn": round(self.m1_dn, 2),
                "m1": self.m1.brief()}


def m1_trend(s: TFState) -> tuple[float, str, bool]:
    """M1 main trend (-1..1): EMA trend, HH/HL vs LH/LL swing structure, last-bars structure, MACD, RSI level."""
    if not s.ready:
        return 0.0, "unknown (no M1 bars yet)", False
    macd = _clip(s.hist / 0.3) * (1.0 if s.macd_ready else 0.5)
    rsi = _clip((s.rsi - 50.0) / 20.0) * (1.0 if s.rsi_ready else 0.0)
    b = 0.36 * s.trend + 0.26 * s.swing + 0.12 * s.structure + 0.14 * macd + 0.12 * rsi
    if s.sbreak and s.sbreak * b < 0:
        b *= 0.5                                   # price just broke the last swing against the trend
    exhausting = s.exhaust != 0 and s.exhaust * b > 0
    if exhausting:
        b *= 0.7
    b = _clip(b)
    supported = abs(b) >= 0.25 and not exhausting
    lab = ("up" if b > 0 else "down") if abs(b) >= 0.2 else "sideways"
    lab += {1: " HH/HL", -1: " LH/LL", 0: ""}[s.swing]
    lab += " (exhausting)" if exhausting else ""
    return b, "M1 " + lab, supported


def rsi_score(s: TFState, d: int) -> float:
    """RSI as a SOFT feature (0..1 for direction d): level on our side of 50, slope and momentum change our
    way; fading when stretched (>75 for BUY, <25 for SELL) and turning back. Never a gate, never a cross wait."""
    if not s.ready or not s.rsi_ready:
        return 0.0
    lvl = _clip(d * (s.rsi - 50.0) / 15.0, 0.0, 1.0)
    slope = _clip(d * s.rsi_slope / 6.0, 0.0, 1.0)
    chg = 1.0 if d * s.rsi_accel > 0 and d * s.rsi_slope > 0 else 0.0
    x = 0.35 * lvl + 0.45 * slope + 0.2 * chg
    stretched = d * (s.rsi - 50.0) > 25.0
    if stretched and d * s.rsi_slope < 0:
        x *= 0.3                                  # overextended and turning back
    return _clip(x, 0.0, 1.0)


def m1_timing(s: TFState, d: int, bias: float) -> float:
    if not s.ready:
        return 0.0
    body = _clip(d * s.body / 0.5, 0.0, 1.0) * min(1.0, 0.5 + s.range_x)
    hist_exp = _clip(d * s.hist_slope / 0.12, 0.0, 1.0)               # histogram expanding our way (no cross needed)
    turn = 1.0 if d * s.hist_accel > 0 and d * s.hist_slope > 0 else 0.0
    vol = 0.0 if s.vol_x is None else _clip((s.vol_x - 1.0) / 1.0, 0.0, 1.0)
    trend = _clip(d * s.trend, 0.0, 1.0)
    cont = _clip(d * bias / 0.5, 0.0, 1.0)
    x = 0.25 * body + 0.21 * hist_exp + 0.08 * turn + 0.13 * vol + 0.09 * trend + 0.12 * cont + 0.12 * rsi_score(s, d)
    if s.exhaust == d:
        x -= 0.2
    return _clip(x, 0.0, 1.0)


class MTF:
    """Per symbol M1 series (M1 only)."""

    def __init__(self):
        self.m1 = TF(60, 300)
        self.polled = {"M1": 0.0}
        self.poll_err = ""

    def on_tick(self, t: float, price: float) -> None:
        self.m1.on_tick(t, price)

    def view(self, now: float) -> MTFView:
        m1 = self.m1.state(now)
        v = MTFView(m1=m1)
        v.bias, v.bias_label, v.supported = m1_trend(m1)
        v.m1_up, v.m1_dn = m1_timing(v.m1, 1, v.bias), m1_timing(v.m1, -1, v.bias)
        return v

    def closed_m1(self) -> list:
        return list(self.m1.bars)
