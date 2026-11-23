"""Online learning from REAL closed bot bursts (pure logic + a small JSON journal).

Bucket = setup: symbol x side vs the M1 main trend x tape regime x spread/ATR regime. Each bucket keeps
statistics of P/L in R (R = realised P/L / risk at open). t30u: memory is NEVER reset - no time decay by
default (half_life_h <= 0) and, besides the local file, a durable remote copy (Render Key Value) survives
redeploys. Decisions:
  * cold bucket (n < min_samples): allowed at normal size (learning by trading);
  * losing bucket (shrunk expectancy < disable_r after min_samples): gated by Thompson
    sampling - usually skipped, sometimes re-explored; decay pulls it back to 'cold';
  * proven bucket (lower confidence bound of expectancy > 0): normal risk sizing x a multiplier
    (1..max_mult) that grows with the expectancy. Never martingale (the trader still caps the lot
    after a losing burst).
No guarantee of profit: this only re-weights what real trades have shown so far.
"""
from __future__ import annotations

import json
import math
import os
import random
import time
from dataclasses import dataclass


@dataclass
class Decision:
    allow: bool
    min_lot: bool
    mult: float
    fast_cut: bool
    state: str
    why: str


def regime(spread: float, atr: float) -> str:
    if atr <= 0:
        return "unknown"
    x = spread / atr
    return "tight" if x < 0.15 else "mid" if x < 0.35 else "wide"


class Learner:
    def __init__(self, path: str = "", min_samples: int = 8, prior_n: float = 4.0, prior_r: float = -0.05,
                 disable_r: float = -0.15, half_life_h: float = 36.0, max_mult: float = 2.0,
                 rng: random.Random | None = None, remote=None, remote_key: str = "rapidburst:learning"):
        self.path = path
        self.remote, self.remote_key = remote, remote_key
        self.remote_status = "off" if remote is None else "connecting"
        self.min_samples, self.prior_n, self.prior_r = min_samples, prior_n, prior_r
        self.disable_r, self.half_life, self.max_mult = disable_r, half_life_h * 3600.0, max_mult
        self.rng = rng or random.Random()
        self.buckets: dict[str, dict] = {}
        self.journal: list[dict] = []
        self.total = 0
        self.load()

    # ------------------------------------------------------------ storage
    def _apply(self, d: dict) -> None:
        self.buckets = d.get("buckets", {})
        self.journal = d.get("journal", [])[-500:]
        self.total = int(d.get("total", len(self.journal)))

    def load(self) -> None:
        local = None
        if self.path and os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    local = json.load(fh)
            except Exception:  # noqa: BLE001  corrupt file -> ignore, never crash trading
                local = None
        remote = None
        if self.remote is not None:
            try:
                raw = self.remote.get(self.remote_key)
                remote = json.loads(raw) if raw else None
                self.remote_status = "ok" if remote else "ok (empty)"
            except Exception as e:  # noqa: BLE001
                self.remote_status = f"unavailable ({type(e).__name__})"
        best = max((x for x in (local, remote) if isinstance(x, dict)), key=lambda x: int(x.get("total", 0)),
                   default=None)
        if best is not None:
            self._apply(best)

    def dump(self) -> dict:
        return {"buckets": self.buckets, "journal": self.journal[-500:], "total": self.total}

    def save(self) -> None:
        data = json.dumps(self.dump())
        if self.path:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(data)
            os.replace(tmp, self.path)
        if self.remote is not None:
            try:
                self.remote.set(self.remote_key, data.encode())
                self.remote_status = "ok"
            except Exception as e:  # noqa: BLE001  durable copy failed: local copy still kept
                self.remote_status = f"unavailable ({type(e).__name__})"

    # -------------------------------------------------------------- stats
    @staticmethod
    def key(sig_type: str, rel: str, reg: str) -> str:
        return f"{sig_type}|{rel}|{reg}"

    def _decayed(self, b: dict, now: float) -> dict:
        if self.half_life <= 0:
            return {"n": b["n"], "sr": b["sr"], "sr2": b["sr2"], "w": b["w"]}    # never forget
        w = 0.5 ** (max(0.0, now - b.get("t", now)) / self.half_life)
        return {"n": b["n"] * w, "sr": b["sr"] * w, "sr2": b["sr2"] * w, "w": b["w"] * w}

    def stats(self, key: str, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        b = self.buckets.get(key)
        if not b:
            return {"n": 0.0, "mean": self.prior_r, "se": 1.0, "win": 0.5, "raw_n": 0}
        d = self._decayed(b, now)
        n = d["n"]
        mean = (d["sr"] + self.prior_n * self.prior_r) / (n + self.prior_n)        # shrunk expectancy (R)
        var = max(0.05, (d["sr2"] / n - (d["sr"] / n) ** 2) if n > 0 else 1.0)
        se = math.sqrt(var / (n + self.prior_n))
        win = (d["w"] + 1.0) / (n + 2.0)
        return {"n": n, "mean": mean, "se": se, "win": win, "raw_n": int(b.get("raw", 0))}

    def record(self, entry: dict, now: float | None = None) -> None:
        now = time.time() if now is None else now
        r = max(-3.0, min(3.0, float(entry.get("r", 0.0))))       # clip outliers (gaps, data errors)
        k = entry["bucket"]
        b = self.buckets.get(k)
        if b:
            d = self._decayed(b, now)
            b.update(n=d["n"] + 1, sr=d["sr"] + r, sr2=d["sr2"] + r * r, w=d["w"] + (1 if r > 0 else 0), t=now)
            b["raw"] = int(b.get("raw", 0)) + 1
        else:
            self.buckets[k] = {"n": 1.0, "sr": r, "sr2": r * r, "w": 1.0 if r > 0 else 0.0, "t": now, "raw": 1}
        self.journal.append(dict(entry, r=round(r, 3), ts=round(now, 1)))
        self.journal = self.journal[-500:]
        self.total += 1
        self.save()

    # ----------------------------------------------------------- decision
    def decide(self, key: str, now: float | None = None) -> Decision:
        s = self.stats(key, now)
        n, mean, se = s["n"], s["mean"], s["se"]
        if n < self.min_samples:
            return Decision(True, False, 1.0, False, "cold",
                            f"learning {key}: new setup ({n:.1f}/{self.min_samples} trades) - normal size")
        if mean < self.disable_r:
            sample = self.rng.gauss(mean, se)
            if sample < 0:
                return Decision(False, True, 0.0, True, "disabled",
                                f"learning {key}: losing bucket (exp {mean:+.2f}R, win {s['win']:.0%}, "
                                f"n {n:.1f}) - skipped")
            return Decision(True, True, 1.0, True, "explore",
                            f"learning {key}: losing bucket re-explored (exp {mean:+.2f}R) - min lot")
        if mean - se > 0:
            mult = min(self.max_mult, 1.0 + mean / 0.5)
            return Decision(True, False, mult, False, "proven",
                            f"learning {key}: proven exp {mean:+.2f}R win {s['win']:.0%} n {n:.1f} - size x{mult:.2f}")
        return Decision(True, False, 1.0, False, "neutral",
                        f"learning {key}: unproven (exp {mean:+.2f}R n {n:.1f}) - normal size")

    def summary(self, now: float | None = None, top: int = 3) -> dict:
        rows = []
        for k in self.buckets:
            s = self.stats(k, now)
            st = ("cold" if s["n"] < self.min_samples else "losing" if s["mean"] < self.disable_r
                  else "proven" if s["mean"] - s["se"] > 0 else "neutral")
            rows.append({"bucket": k, "n": round(s["n"], 1), "raw_n": s["raw_n"], "win": round(s["win"], 2),
                         "exp_r": round(s["mean"], 3), "state": st})
        rows.sort(key=lambda x: x["exp_r"], reverse=True)
        return {"trades": self.total, "buckets": len(rows), "top": rows[:top],
                "bottom": [r for r in rows[::-1][:top] if r not in rows[:top]],
                "min_samples": self.min_samples, "memory": self.remote_status,
                "decay": "none (never reset)" if self.half_life <= 0 else f"half-life {self.half_life / 3600:g} h"}
