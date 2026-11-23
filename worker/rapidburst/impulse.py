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


def _pressure(ticks, now, seconds):
    win = [tk for tk in ticks if tk.t >= now - seconds]
    up = sum(1 for a, b in zip(win, win[1:]) if b.mid > a.mid)
    dn = sum(1 for a, b in zip(win, win[1:]) if b.mid < a.mid)
    return (up - dn) / (up + dn) if (up + dn) else 0.0, len(win)


def detect_early(ticks: list, now: float, atr: float, spread: float, cfg) -> Impulse:
    """Impulse at its START (scalp mode): fast + ACCELERATING tick velocity, one-sided order flow, a real but SMALL
    displacement from where the move began, and the anti-chase filter: refuse when price already moved more than
    scalp_max_bar_atr from the M1 bar open or scalp_max_swing_atr from the recent swing."""
    if len(ticks) < 4 or atr <= 0:
        return Impulse(reason="impulse: not enough data", status={"atr": atr})
    last = ticks[-1]
    W = cfg.scalp_vel_seconds
    m1, m2 = _mid_at(ticks, now - W), _mid_at(ticks, now - 2 * W)
    if m1 is None or m2 is None:
        return Impulse(reason="impulse: not enough data", status={"atr": atr})
    v1, v2 = last.mid - m1, m1 - m2
    bar_start = math.floor(now / 60.0) * 60.0
    bar_open = _mid_at(ticks, bar_start) or next((tk.mid for tk in ticks if tk.t >= bar_start), last.mid)
    bar_move = last.mid - bar_open
    pressure, nwin = _pressure(ticks, now, cfg.scalp_flow_seconds)
    sw = [tk.mid for tk in ticks if tk.t >= now - cfg.scalp_swing_seconds] or [last.mid]
    st5 = [tk.mid for tk in ticks if tk.t >= now - cfg.scalp_start_seconds] or [last.mid]
    st = {"atr": round(atr, 5), "vel_atr": round(v1 / atr, 3), "prev_vel_atr": round(v2 / atr, 3),
          "bar_move_atr": round(bar_move / atr, 2), "pressure": round(pressure, 2), "ticks_w": nwin,
          "bar_age": round(now - bar_start)}
    d = 1 if v1 > 0 else -1 if v1 < 0 else 0
    if d == 0:
        return Impulse(reason="impulse: flat", status=st)
    from_swing = d * (last.mid - (min(sw) if d > 0 else max(sw)))
    from_start = d * (last.mid - (min(st5) if d > 0 else max(st5)))
    st.update(dir=d, from_swing_atr=round(from_swing / atr, 2), from_start_atr=round(from_start / atr, 2))
    checks = [
        (d * v1 >= max(cfg.scalp_vel_atr * atr, 0.5 * spread), f"velocity {d * v1 / atr:.3f} ATR too slow"),
        (d * v1 >= d * v2 * cfg.scalp_min_accel, "decelerating (velocity below previous window)"),
        (d * pressure >= cfg.scalp_min_pressure, f"order flow {d * pressure:+.2f} < {cfg.scalp_min_pressure}"),
        (from_start >= max(cfg.scalp_start_spread_mult * spread, 0.02 * atr), "no real start yet (move <= spread)"),
        (d * bar_move <= cfg.scalp_max_bar_atr * atr, f"anti-chase: already {d * bar_move / atr:.2f} ATR from bar open"),
        (from_swing <= cfg.scalp_max_swing_atr * atr, f"anti-chase: already {from_swing / atr:.2f} ATR from swing"),
    ]
    for ok, why in checks:
        if not ok:
            return Impulse(reason=f"impulse: {why}", status=dict(st))
    strength = (0.4 * math.tanh(d * v1 / (0.1 * atr)) + 0.3 * max(0.0, d * pressure)
                + 0.3 * (1.0 - min(1.0, max(0.0, from_swing) / max(cfg.scalp_max_swing_atr * atr, 1e-9))))
    strength = max(0.0, min(1.0, strength))
    side = "BUY" if d > 0 else "SELL"
    return Impulse(dir=d, strength=strength,
                   reason=f"impulse-early {side} {strength:.2f} (vel {d * v1 / atr:.3f}ATR acc, flow {d * pressure:+.2f}, "
                          f"bar {d * bar_move / atr:+.2f}ATR, swing {from_swing / atr:.2f}ATR)",
                   status=dict(st, strength=round(strength, 2)))


def detect_reversal(ticks: list, now: float, d: int, atr: float, spread: float, peak_px, peak_v: float, cfg) -> dict:
    """Is the force of the move (direction d) about to reverse? Flags: vel_flip (tick velocity changed sign),
    decay (velocity fell below scalp_decay_frac of its peak), press (opposing tick pressure), wick (price pulled
    back from the favourable extreme). Pure tick logic."""
    if not ticks or atr <= 0:
        return {"vel_flip": False, "decay": False, "press": False, "wick": False, "v": 0.0}
    last = ticks[-1]
    m1 = _mid_at(ticks, now - cfg.scalp_vel_seconds)
    v1 = (last.mid - m1) if m1 is not None else 0.0
    pressure, _ = _pressure(ticks, now, cfg.scalp_flow_seconds)
    flip = d * v1 <= -max(cfg.scalp_rev_vel_atr * atr, 0.5 * spread)
    decay = peak_v > 0 and d * v1 < cfg.scalp_decay_frac * peak_v
    press = d * pressure <= -cfg.scalp_rev_pressure
    wick = peak_px is not None and d * (peak_px - last.mid) >= max(cfg.scalp_wick_atr * atr, spread)
    return {"vel_flip": flip, "decay": decay, "press": press, "wick": wick, "v": d * v1, "pressure": pressure}
