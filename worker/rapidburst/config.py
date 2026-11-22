"""Engine / risk configuration.

All values are conservative defaults intended for a DEMO account.  They can be
overridden with a JSON file (``RB_CONFIG_FILE``) on the worker machine.  The
UI cannot change risk limits and can never enable LIVE trading.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict, fields


@dataclass
class EngineConfig:
    # --- tick windows (seconds) ---
    fast_window: float = 3.0          # displacement / velocity window
    slow_window: float = 15.0         # short-term structure window
    vol_window: float = 120.0         # volatility normalisation window
    breakout_lookback: float = 20.0   # micro-structure range lookback
    min_ticks_warmup: int = 40        # ticks required before any decision
    warmup_seconds: float = 60.0      # seconds of tick history required before any decision
    min_tick_rate: float = 0.5        # ticks/sec required (liquidity / session open)
    max_tick_age: float = 10.0        # seconds; older last tick => market idle -> HOLD

    # --- entry scoring (no RSI, no fixed confirmation chain) ---
    min_impulse_units: float = 3.0  # fast displacement (vol units) that counts as a burst
    min_pressure: float = 0.35      # one-sided tick pressure that must accompany the impulse
    entry_threshold: float = 0.60     # |score| needed to enter (score in [-1, 1])
    add_threshold: float = 0.70       # |score| needed to add a second/third order
    exhaustion_z: float = 3.2         # price z-score vs. 60s mean => extended, do not chase
    chase_disp_units: float = 4.0     # slow displacement (vol units) beyond which we refuse to chase

    # --- spread / execution quality ---
    max_spread_bps: float = 5.0       # spread cap relative to price (basis points of mid): symbol-agnostic
    max_spread_points: float = 0.0    # optional extra absolute cap in points (0 = off; legacy / per-deployment)
    max_spread_vol_ratio: float = 3.0 # spread must be < ratio * volatility unit (3s typical move)
    spread_spike_ratio: float = 2.0   # spread > ratio * median spread => skip
    max_avg_slippage_vu: float = 0.75 # recent fills avg slippage cap in volatility units -> pause entries
    deviation_points: int = 20        # minimum slippage accepted by the server; raised per symbol from spread/volatility

    # --- exits ---
    sl_vol_units: float = 6.0         # protective stop = units * vol unit (>= stops level)
    min_sl_spread_mult: float = 3.0   # stop never tighter than spread * mult
    tp_vol_units: float = 12.0        # server-side backstop TP (normal exits are earlier)
    profit_trigger_units: float = 1.8 # start protecting profit once reached
    giveback_frac: float = 0.40       # close if profit gives back this fraction of the peak
    weak_momentum_close_units: float = 0.6  # in profit & momentum flips => close
    reversal_score: float = 0.55      # opposite score this strong => close
    max_hold_seconds: float = 180.0   # time stop for trades that do not work
    stale_loss_units: float = 2.5     # after half hold time and still losing this much => close

    # --- entry pacing / duplicate prevention ---
    cooldown_seconds: float = 8.0     # min time between entries on one symbol
    loss_cooldown_seconds: float = 45.0
    max_entries_per_minute: int = 4   # per symbol
    max_consecutive_losses: int = 4   # then pause symbol for loss_pause_seconds
    loss_pause_seconds: float = 600.0

    # --- risk ---
    risk_per_trade_pct: float = 0.25  # % of equity lost if the SL is hit
    max_lot_per_order: float = 1.0
    max_positions_total: int = 4
    max_positions_per_symbol: int = 2
    max_total_lots: float = 2.0
    max_total_notional_x: float = 30.0  # sum of open notional (account ccy) <= x * equity, when computable
    max_total_risk_pct: float = 1.0   # sum of open SL risk across bot positions
    daily_loss_limit_pct: float = 3.0 # bot daily P/L <= -x% of day-start balance => halt
    equity_drawdown_pct: float = 5.0  # equity below day peak by x% => halt
    min_margin_level_pct: float = 300.0
    margin_use_max_pct: float = 30.0  # new order margin must be < x% of free margin

    # --- loop ---
    loop_interval: float = 0.15
    snapshot_interval: float = 1.0
    magic: int = 26112201
    comment_prefix: str = "RBurst"
    timezone: str = "Asia/Bangkok"
    stop_closes_positions: bool = True

    @classmethod
    def load(cls) -> "EngineConfig":
        cfg = cls()
        path = os.environ.get("RB_CONFIG_FILE")
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            names = {f.name for f in fields(cls)}
            for k, v in data.items():
                if k in names:
                    setattr(cfg, k, type(getattr(cfg, k))(v))
        return cfg

    def public(self) -> dict:
        return asdict(self)
