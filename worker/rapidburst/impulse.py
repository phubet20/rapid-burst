"""M1 impulse-bar detector from per-second tick order flow (pure logic, real ticks only).

A bar is 'starting to surge' when, inside the current M1 bar,
  * the last ~3 s moved fast in one direction (velocity vs the M1 ATR) and is not decelerating,
  * tick order flow agrees (up-ticks vs down-ticks over the last few seconds),
  * the bar is expanding in that direction (move from the bar open vs the M1 ATR),
  * the move is larger than the spread (otherwise the spread eats it),
  * and it is not already exhausted (bar move not beyond impulse_max_bar_atr x ATR).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class Impulse:
    dir: int = 0
    strength: float = 0.0
    reason: str = ""
    status: dict = field(default_factory=dict)


def _mid_at(ticks, t):
    m = None
    for tk in ticks:
        if tk.t <= t:
            m = tk.mid
        else:
            break
    return m


def detect(ticks: list, now: float, atr: float, spread: float, cfg) -> Impulse:
    if len(ticks) < 6 or atr <= 0:
        return Impulse(reason="impulse: not enough data", status={"atr": atr})
    last = ticks[-1]
    bar_start = math.floor(now / 60.0) * 60.0
    bar_open = _mid_at(ticks, bar_start) or next((tk.mid for tk in ticks if tk.t >= bar_start), last.mid)
    in_bar = [tk.mid for tk in ticks if tk.t >= bar_start] or [last.mid]
    bar_move = last.mid - bar_open
    bar_range = max(in_bar + [bar_open]) - min(in_bar + [bar_open])
    w = cfg.impulse_flow_seconds
    win = [tk for tk in ticks if tk.t >= now - w]
    up = sum(1 for a, b in zip(win, win[1:]) if b.mid > a.mid)
    dn = sum(1 for a, b in zip(win, win[1:]) if b.mid < a.mid)
    pressure = (up - dn) / (up + dn) if (up + dn) else 0.0
    m3 = _mid_at(ticks, now - 3.0)
    m6 = _mid_at(ticks, now - 6.0)
    vel = last.mid - m3 if m3 is not None else 0.0
    prev = (m3 - m6) if (m3 is not None and m6 is not None) else 0.0
    move_w = last.mid - (win[0].mid if win else last.mid)
    st = {"atr": round(atr, 5), "bar_range_atr": round(bar_range / atr, 2), "bar_move_atr": round(bar_move / atr, 2),
          "vel3_atr": round(vel / atr, 2), "pressure": round(pressure, 2), "ticks_w": len(win),
          "bar_age": round(now - bar_start)}
    d = 1 if vel > 0 else -1 if vel < 0 else 0
    if d == 0:
        return Impulse(reason="impulse: flat", status=st)
    checks = [
        (d * vel >= cfg.impulse_vel_atr * atr, f"3s velocity {d * vel / atr:.2f} < {cfg.impulse_vel_atr} ATR"),
        (d * pressure >= cfg.impulse_min_pressure, f"order flow {d * pressure:+.2f} < {cfg.impulse_min_pressure}"),
        (max(d * bar_move, d * move_w) >= cfg.impulse_bar_atr * atr,
         f"bar expansion {max(d * bar_move, d * move_w) / atr:.2f} < {cfg.impulse_bar_atr} ATR"),
        (max(d * bar_move, d * move_w) >= cfg.impulse_min_spread_mult * spread, "move smaller than spread"),
        (d * bar_move <= cfg.impulse_max_bar_atr * atr, f"bar already {d * bar_move / atr:.2f} ATR (exhausted)"),
        (d * vel >= d * prev * cfg.impulse_min_accel, "decelerating"),
    ]
    for ok, why in checks:
        if not ok:
            return Impulse(reason=f"impulse: {why}", status=dict(st, dir=d))
    strength = (0.4 * math.tanh(d * vel / (0.25 * atr)) + 0.3 * max(0.0, d * pressure)
                + 0.3 * math.tanh(max(d * bar_move, d * move_w) / (0.5 * atr)))
    strength = max(0.0, min(1.0, strength))
    side = "BUY" if d > 0 else "SELL"
    return Impulse(dir=d, strength=strength,
                   reason=f"impulse {side} {strength:.2f} (vel {d * vel / atr:.2f}ATR, flow {d * pressure:+.2f}, "
                          f"bar {d * bar_move / atr:+.2f}ATR)", status=dict(st, dir=d, strength=round(strength, 2)))
