"""Position sizing, exposure limits and entry pacing (pure logic).

Hard rules enforced here:
  * every order has a protective stop; size is derived from the stop distance
  * never add to a losing position (no martingale / averaging / grid)
  * bounded positions per symbol and in total, bounded lots and open risk
  * daily loss limit and equity drawdown stop halt the bot for the day
  * cooldowns, per-minute entry caps and consecutive-loss pauses
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Iterable, Optional

from .config import EngineConfig


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    n = math.floor(value / step + 1e-9)
    decimals = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
    return round(n * step, decimals + 2)


def normalize_volume(volume: float, vmin: float, vmax: float, step: float) -> float:
    v = floor_to_step(min(volume, vmax), step)
    if v + 1e-12 < vmin:
        return 0.0
    return v


def loss_per_lot(sl_distance: float, tick_size: float, tick_value_loss: float) -> float:
    """Account-currency loss for 1.0 lot if price moves sl_distance against us."""
    if tick_size <= 0 or tick_value_loss <= 0:
        return 0.0
    return sl_distance / tick_size * tick_value_loss


@dataclass
class SizeResult:
    volume: float
    risk_money: float
    note: str


def size_position(equity: float, cfg: EngineConfig, sl_distance: float, tick_size: float,
                  tick_value_loss: float, vmin: float, vmax: float, vstep: float) -> SizeResult:
    lpl = loss_per_lot(sl_distance, tick_size, tick_value_loss)
    if lpl <= 0 or equity <= 0:
        return SizeResult(0.0, 0.0, "invalid symbol tick value / equity")
    budget = equity * cfg.risk_per_trade_pct / 100.0
    raw = budget / lpl
    vol = normalize_volume(min(raw, cfg.max_lot_per_order), vmin, vmax, vstep)
    if vol == 0.0:
        # risk-based lot is below the broker minimum (tiny account): NO minimum-balance rule - trade the
        # broker minimum lot (free margin permitting, checked later). An optional cap can be configured.
        if vmin > cfg.max_lot_per_order:
            return SizeResult(0.0, vmin * lpl, f"min lot {vmin:g} exceeds max_lot_per_order {cfg.max_lot_per_order:g}")
        if cfg.small_account_max_loss_pct > 0:
            cap = max(budget * 1.5, equity * cfg.small_account_max_loss_pct / 100.0)
            if vmin * lpl > cap:
                return SizeResult(0.0, vmin * lpl, f"min lot SL loss {vmin * lpl:.2f} > small-account cap {cap:.2f} "
                                                   f"({cfg.small_account_max_loss_pct:g}% equity)")
        return SizeResult(vmin, vmin * lpl, f"min lot floor (risk {vmin * lpl:.2f} = "
                                            f"{vmin * lpl / equity * 100:.1f}% of equity)")
    return SizeResult(vol, vol * lpl, "ok")


@dataclass
class OpenPos:
    symbol: str
    side: str
    volume: float
    profit: float
    risk_money: float
    notional: float = 0.0     # account-currency notional (0 = not computable)


@dataclass
class AccountView:
    balance: float
    equity: float
    margin_free: float
    margin_level: float  # percent, 0 when no margin used


class RiskGuard:
    """Account-wide guards. Halts are sticky for the trading day."""

    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.day_key: Optional[str] = None
        self.day_start_balance = 0.0
        self.day_peak_equity = 0.0
        self.halted = False
        self.halt_reason = ""

    def new_day(self, day_key: str, balance: float, equity: float) -> None:
        self.day_key = day_key
        self.day_start_balance = balance
        self.day_peak_equity = equity
        self.halted = False
        self.halt_reason = ""

    def update(self, day_key: str, acct: AccountView, bot_daily_pnl: float) -> Optional[str]:
        """Update trackers; returns a halt reason when a hard limit is newly breached."""
        if self.day_key != day_key:
            self.new_day(day_key, acct.balance, acct.equity)
        self.day_peak_equity = max(self.day_peak_equity, acct.equity)
        if self.halted:
            return None
        lim = self.day_start_balance * self.cfg.daily_loss_limit_pct / 100.0
        if lim > 0 and bot_daily_pnl <= -lim:
            self.halted, self.halt_reason = True, f"daily loss limit hit ({bot_daily_pnl:.2f} <= -{lim:.2f})"
            return self.halt_reason
        dd_floor = self.day_peak_equity * (1 - self.cfg.equity_drawdown_pct / 100.0)
        if self.cfg.equity_drawdown_pct > 0 and acct.equity < dd_floor:
            self.halted, self.halt_reason = True, (
                f"equity drawdown stop ({acct.equity:.2f} < {dd_floor:.2f}, peak {self.day_peak_equity:.2f})")
            return self.halt_reason
        return None

    def check_entry(self, symbol: str, side: str, positions: Iterable[OpenPos], acct: AccountView,
                    new_volume: float, new_risk: float, new_margin: float, add_score_ok: bool,
                    new_notional: float = 0.0, batch_leg: bool = False,
                    floor_mode: bool = False) -> tuple[bool, str]:
        cfg = self.cfg
        if self.halted:
            return False, f"halted: {self.halt_reason}"
        pos = list(positions)
        if len(pos) >= cfg.max_positions_total:
            return False, f"max positions {cfg.max_positions_total} reached"
        mine = [p for p in pos if p.symbol == symbol]
        if len(mine) >= cfg.max_positions_per_symbol:
            return False, f"max {cfg.max_positions_per_symbol} positions on {symbol}"
        if any(p.side != side for p in mine):
            return False, "opposite position open on symbol (no hedging)"
        if mine and not batch_leg:
            # adding is allowed only into strength: every existing leg must be in profit
            # (batch_leg = the 2nd..Nth order of a burst batch being opened right now on one signal)
            if any(p.profit <= 0 for p in mine):
                return False, "no averaging into a losing position"
            if not add_score_ok:
                return False, "add requires stronger momentum"
        if sum(p.volume for p in pos) + new_volume > cfg.max_total_lots + 1e-9:
            return False, f"max total lots {cfg.max_total_lots} would be exceeded"
        if new_notional > 0 and cfg.max_total_notional_x > 0:
            cap_n = acct.equity * cfg.max_total_notional_x
            if sum(p.notional for p in pos) + new_notional > cap_n + 1e-9:
                return False, f"max total exposure {cfg.max_total_notional_x:.0f}x equity would be exceeded"
        if floor_mode:
            # broker-minimum lot on a tiny account: risk %/margin-use rules cannot apply (the lot cannot be
            # smaller); only real free margin matters - the broker would reject the order otherwise
            if new_margin > acct.margin_free + 1e-9:
                return False, (f"insufficient free margin for the minimum lot: needs {new_margin:.2f}, "
                               f"free {acct.margin_free:.2f}")
            return True, "ok (min-lot floor)"
        risk_cap = acct.equity * cfg.max_total_risk_pct / 100.0
        if sum(p.risk_money for p in pos) + new_risk > risk_cap + 1e-9:
            return False, f"max open risk {risk_cap:.2f} would be exceeded"
        if acct.margin_level and acct.margin_level < cfg.min_margin_level_pct:
            return False, f"margin level {acct.margin_level:.0f}% too low"
        if new_margin > acct.margin_free * cfg.margin_use_max_pct / 100.0:
            return False, "insufficient free margin for order"
        return True, "ok"


class EntryPacer:
    """Per-symbol pacing: cooldowns, entries/minute cap, loss streak pause, in-flight lock."""

    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.last_entry: dict[str, float] = {}
        self.recent: dict[str, deque] = {}
        self.loss_until: dict[str, float] = {}
        self.streak: dict[str, int] = {}
        self.inflight: set[str] = set()

    def can_enter(self, symbol: str, now: float) -> tuple[bool, str]:
        cfg = self.cfg
        if symbol in self.inflight:
            return False, "order in flight"
        until = self.loss_until.get(symbol, 0.0)
        if now < until:
            return False, f"loss cooldown {until - now:.0f}s"
        last = self.last_entry.get(symbol)
        if last is not None and now - last < cfg.cooldown_seconds:
            return False, f"cooldown {cfg.cooldown_seconds - (now - last):.0f}s"
        q = self.recent.setdefault(symbol, deque())
        while q and now - q[0] > 60.0:
            q.popleft()
        if len(q) >= cfg.max_entries_per_minute:
            return False, "entries/minute cap"
        return True, "ok"

    def on_entry(self, symbol: str, now: float) -> None:
        self.last_entry[symbol] = now
        self.recent.setdefault(symbol, deque()).append(now)

    def on_close(self, symbol: str, profit: float, now: float) -> None:
        cfg = self.cfg
        if profit < 0:
            n = self.streak.get(symbol, 0) + 1
            self.streak[symbol] = n
            pause = cfg.loss_pause_seconds if n >= cfg.max_consecutive_losses else cfg.loss_cooldown_seconds
            self.loss_until[symbol] = max(self.loss_until.get(symbol, 0.0), now + pause)
            if n >= cfg.max_consecutive_losses:
                self.streak[symbol] = 0
        else:
            self.streak[symbol] = 0
