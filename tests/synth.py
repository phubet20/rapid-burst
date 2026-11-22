"""SYNTHETIC tick generator for tests and replay only (not market data)."""
from __future__ import annotations

import math
import random


def gen_ticks(seed: int, start: float, seconds: float, base: float = 2650.0, point: float = 0.01,
              spread_pts: float = 18, burst_every: float = 90.0, chop: bool = True):
    """Random-walk ticks with occasional momentum bursts, reversals and spread spikes.
    Returns list of (t, bid, ask)."""
    rnd = random.Random(seed)
    t = start
    mid = base
    out = []
    drift = 0.0
    drift_until = 0.0
    next_burst = start + rnd.uniform(30, burst_every)
    while t < start + seconds:
        t += rnd.uniform(0.15, 0.6)
        if t >= next_burst:
            drift = rnd.choice([-1, 1]) * rnd.uniform(0.04, 0.09)   # price units per tick
            drift_until = t + rnd.uniform(5, 14)
            next_burst = t + rnd.uniform(burst_every * 0.5, burst_every * 1.5)
        d = drift if t < drift_until else 0.0
        if t >= drift_until and drift != 0.0 and rnd.random() < 0.02:
            drift = 0.0
        sigma = 0.035 if chop else 0.02
        mid += d + rnd.gauss(0, sigma)
        sp = spread_pts
        if rnd.random() < 0.01:
            sp *= rnd.uniform(2.5, 5)
        half = sp * point / 2
        bid = round(mid - half, 2)
        ask = round(mid + half, 2)
        if ask <= bid:
            ask = bid + point
        out.append((t, bid, ask))
    return out


def burst_series(start: float, direction: int, pre_seconds: float = 150, burst_seconds: float = 6,
                 step: float = 0.08, point: float = 0.01, base: float = 2650.0, seed: int = 1):
    """Quiet market then a clean directional burst (deterministic-ish)."""
    rnd = random.Random(seed)
    t, mid, out = start, base, []
    while t < start + pre_seconds:
        t += 0.3
        mid += rnd.gauss(0, 0.03)
        out.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    end = t + burst_seconds
    while t < end:
        t += 0.25
        mid += direction * step + rnd.gauss(0, 0.01)
        out.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return out
