"""Position sizing, exposure limits and entry pacing (pure logic).

Hard rules enforced here:
  * every order has a protective stop; size is derived from the stop distance (t30u: aggressive defaults
    requested by the owner - up to 50 % of equity per basket, 100 % across all open baskets, margin <= 90 %)
  * never add to a losing position (no martingale / averaging / grid)
  * bounded positions per symbol and in total, bounded lots and open risk
  * NO all-day daily-loss halt: equity drawdown => SAFE MODE (flatten, pause new entries, resume reduced)
  * NO cooldowns / candle locks: an in-flight lock, an entries/minute API guard and a loss streak that only
    raises the signal threshold (never the size)
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
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
    """Account-wide guards. There is NO all-day daily-loss halt. An equity drawdown from the session peak puts
    the bot in SAFE MODE (t32u: peak = closed balance; only NEW entries pause, open baskets keep their stops), entries pause (5 min, doubling on repeats, max 30 min),
    then trading resumes from a new reference peak with reduced size (single min-lot batches for a while)."""

    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.peak_equity = 0.0
        self.halted = False            # True while SAFE MODE pauses new entries
        self.halt_reason = ""
        self.safe_until = 0.0
        self.safe_count = 0
        self.need_confirm = False      # t32u: after safe mode, the next entry needs fresh M1 structure
        self.session_peak = 0.0
        self.catastrophic = False      # equity below safe_flatten_pct of the peak -> flatten (last resort)
        self.probe_left = 0            # batches still forced to one min-lot order after a safe-mode pause
        self.floor_risk = 0.0          # largest min-lot-floor BASKET stop risk this session (money)

    def reset_session(self, equity: float) -> None:
        self.peak_equity = equity
        self.halted, self.halt_reason = False, ""
        self.safe_until, self.safe_count, self.probe_left = 0.0, 0, 0
        self.need_confirm, self.catastrophic = False, False
        self.session_peak = equity
        self.floor_risk = 0.0

    def dd_limit(self) -> float:
        """Drawdown (money) that triggers safe mode: x% of the peak, but on a min-lot-floor account never
        smaller than `safe_min_stopouts` x the largest basket stop risk (one normal basket stop must not trip
        it), capped at `safe_dd_cap_pct` of the peak (catastrophic protection stays)."""
        cfg = self.cfg
        lim = self.peak_equity * cfg.equity_drawdown_pct / 100.0
        if self.floor_risk > 0:
            lim = max(lim, min(cfg.safe_min_stopouts * self.floor_risk, self.peak_equity * cfg.safe_dd_cap_pct / 100.0))
        return lim

    def update(self, now: float, acct: AccountView) -> Optional[tuple]:
        """Returns ("safe", msg) when safe mode starts, ("resume", msg) when it ends, else None.
        t32u: the reference peak is the CLOSED-equity peak (balance), never a floating spike - an open basket's
        unrealised profit cannot set a peak that the same basket's normal pullback then "loses". Safe mode only
        PAUSES NEW ENTRIES (each open basket keeps its own structure stop / trail); after the pause the first
        entry needs a fresh M1 structure confirmation (`need_confirm`). `catastrophic` (flatten) is only set when
        equity falls `safe_flatten_pct` below the peak."""
        if acct.equity <= 0:
            return None
        # closed equity: the balance, but never above current equity (an open loss is not a "peak")
        bal = min(acct.balance, acct.equity) if acct.balance > 0 else acct.equity
        self.peak_equity = max(self.peak_equity, bal)
        self.session_peak = max(self.session_peak, bal)            # never re-based by a safe-mode resume
        fl = self.cfg.safe_flatten_pct
        if (fl > 0 and not self.catastrophic and self.session_peak > 0
                and acct.equity < self.session_peak * (1 - fl / 100.0)):
            # last resort: flatten bot positions (trader) and pause entries for the maximum pause, then restart
            # from the new balance (no all-day stop)
            self.catastrophic, self.halted = True, True
            self.safe_until = max(self.safe_until, now + self.cfg.safe_pause_max_s)
            self.halt_reason = (f"CATASTROPHIC: equity {acct.equity:.2f} is more than {fl:g}% below the session peak "
                                f"{self.session_peak:.2f} - bot positions closed, entries paused "
                                f"{self.cfg.safe_pause_max_s / 60:.0f} min")
            return "safe", self.halt_reason
        if self.halted:
            if now < self.safe_until:
                return None
            if self.catastrophic:
                self.catastrophic, self.session_peak = False, bal
            self.halted, self.halt_reason = False, ""
            self.peak_equity = bal                          # new reference: no instant re-trigger
            self.probe_left = self.cfg.safe_probe_batches
            self.need_confirm = True
            return "resume", (f"SAFE MODE ended - entries resume after a FRESH M1 structure confirmation "
                              f"(swing break / HH-HL or LH-LL with the trade), new reference {bal:.2f}")
        dd = self.cfg.equity_drawdown_pct
        lim = self.dd_limit()
        if dd > 0 and self.peak_equity > 0 and acct.equity < self.peak_equity - lim:
            pause = min(self.cfg.safe_pause_s * (2 ** self.safe_count), self.cfg.safe_pause_max_s)
            self.safe_count += 1
            self.safe_until = now + pause
            self.halted = True
            self.halt_reason = (f"SAFE MODE: equity {acct.equity:.2f} is {lim:.2f} ({lim / self.peak_equity * 100:.1f}%) "
                                f"below the closed-balance peak {self.peak_equity:.2f} - NEW entries paused "
                                f"{pause / 60:.0f} min (open baskets keep their stops)")
            return "safe", self.halt_reason
        return None

    def check_entry(self, symbol: str, side: str, positions: Iterable[OpenPos], acct: AccountView,
                    new_volume: float, new_risk: float, new_margin: float, add_score_ok: bool,
                    new_notional: float = 0.0, batch_leg: bool = False,
                    floor_mode: bool = False) -> tuple[bool, str]:
        cfg = self.cfg
        if self.halted:
            return False, f"safe mode: {self.halt_reason}"
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
            # broker-minimum lot on a small account: the per-order % cannot apply (the lot cannot be smaller); the
            # basket budget is sized by the trader, here only the TOTAL stop risk of all open bot orders (every
            # symbol) plus this one must stay <= max_total_risk_pct of equity
            cap = acct.equity * cfg.max_total_risk_pct / 100.0
            tot = sum(p.risk_money for p in pos) + new_risk
            if tot > cap + 1e-9:
                return False, (f"total stop-risk cap: open stop risk {tot:.2f} > {cfg.max_total_risk_pct:g}% "
                               f"of equity ({cap:.2f})")
            if new_margin > acct.margin_free + 1e-9:
                return False, (f"insufficient free margin for the minimum lot: needs {new_margin:.2f}, "
                               f"free {acct.margin_free:.2f}")
            return True, "ok (min-lot floor, total stop-risk cap)"
        risk_cap = acct.equity * cfg.max_total_risk_pct / 100.0
        if sum(p.risk_money for p in pos) + new_risk > risk_cap + 1e-9:
            return False, f"max open risk {risk_cap:.2f} would be exceeded"
        if acct.margin_level and acct.margin_level < cfg.min_margin_level_pct:
            return False, f"margin level {acct.margin_level:.0f}% too low"
        if new_margin > acct.margin_free * cfg.margin_use_max_pct / 100.0:
            return False, "insufficient free margin for order"
        return True, "ok"


class EntryPacer:
    """Per-symbol pacing for CONTINUOUS re-entry: no fixed cooldowns. In-flight lock, an entries/minute
    API guard, and a loss streak that only raises the signal threshold (never the size)."""

    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.last_entry: dict[str, float] = {}
        self.recent: dict[str, deque] = {}
        self.streak: dict[str, int] = {}
        self.inflight: set[str] = set()

    def can_enter(self, symbol: str, now: float) -> tuple[bool, str]:
        if symbol in self.inflight:
            return False, "order in flight"
        q = self.recent.setdefault(symbol, deque())
        while q and now - q[0] > 60.0:
            q.popleft()
        if len(q) >= self.cfg.max_entries_per_minute:
            return False, "entries/minute cap"
        return True, "ok"

    def threshold_add(self, symbol: str) -> float:
        return self.cfg.loss_streak_threshold_add * min(3, self.streak.get(symbol, 0))

    def on_entry(self, symbol: str, now: float) -> None:
        self.last_entry[symbol] = now
        self.recent.setdefault(symbol, deque()).append(now)

    def on_close(self, symbol: str, profit: float, now: float) -> None:
        self.streak[symbol] = self.streak.get(symbol, 0) + 1 if profit < 0 else 0
