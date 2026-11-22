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
    min_ticks_warmup: int = 4         # ticks required before any decision (seeded from M1 candles)
    warmup_seconds: float = 2.5       # seconds of tick history required before any decision
    min_tick_rate: float = 0.3        # ticks/sec required (TickerAll BTCUSDm measured 0.7-1.2/s)
    max_tick_age: float = 10.0        # seconds; older last tick => market idle -> HOLD

    # --- entry scoring (no RSI, no fixed confirmation chain) ---
    min_impulse_units: float = 3.0  # fast displacement (vol units) that counts as a burst
    min_pressure: float = 0.35      # one-sided tick pressure that must accompany the impulse
    entry_threshold: float = 0.42     # |score| needed to enter (score in [-1, 1])
    add_threshold: float = 0.70       # |score| needed to add a second/third order
    exhaustion_z: float = 3.2         # price z-score vs. 60s mean => extended, do not chase
    chase_disp_units: float = 4.0     # slow displacement (vol units) beyond which we refuse to chase

    # --- spread / execution quality ---
    max_spread_bps: float = 5.0       # spread cap relative to price (basis points of mid): symbol-agnostic
    max_spread_points: float = 0.0    # optional extra absolute cap in points (0 = off; legacy / per-deployment)
    max_spread_vol_ratio: float = 3.0 # spread must be < ratio * volatility unit (3s typical move)
    spread_spike_ratio: float = 2.0   # spread > ratio * median spread => skip
    max_avg_slippage_vu: float = 1.5  # recent fills avg slippage cap in volatility units -> pause entries
    slippage_window_s: float = 90.0   # only fills this recent count (the gate can no longer stick forever)
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
    cooldown_seconds: float = 1.5     # min time between entries on one symbol
    loss_cooldown_seconds: float = 2.0
    max_entries_per_minute: int = 30  # per symbol
    max_consecutive_losses: int = 4   # then pause symbol for loss_pause_seconds
    loss_pause_seconds: float = 20.0   # was 600: a losing streak no longer parks the bot for 10 min

    # --- risk ---
    risk_per_trade_pct: float = 0.25  # % of equity lost if the SL is hit
    max_lot_per_order: float = 1.0
    max_positions_total: int = 8
    max_positions_per_symbol: int = 5
    max_total_lots: float = 2.0
    max_total_notional_x: float = 500.0  # sum of open notional <= x * equity (BTC min lot on $10 = ~90x; SL caps the loss)
    max_total_risk_pct: float = 15.0  # sum of open SL risk across bot positions (% equity)
    small_account_max_loss_pct: float = 0.0  # 0 = NO minimum-balance rule: min lot always allowed (free margin permitting); >0 = optional cap
    batch_tiers: str = "5:1,20:2,50:3,200:4"  # equity below X -> at most N orders per batch (above last: batch_max)
    daily_loss_limit_pct: float = 0.0   # 0 = OFF (user choice). RB_DAILY_LOSS_PCT=x re-enables: daily P/L <= -x% => halt
    equity_drawdown_pct: float = 0.0    # 0 = OFF (user choice). RB_MAX_DD_PCT=x re-enables: equity x% below day peak => halt

    # --- protection WITHOUT a broker-side stop-loss (default): the engine closes positions itself ---
    broker_sl: bool = True           # real broker SL on every order at open (RB_BROKER_SL=0 to disable)
    emergency_sl: bool = False       # RB_EMERGENCY_SL=1: far disaster SL (emergency_sl_units vol units) only
    emergency_sl_units: float = 40.0
    batch_max_loss_pct: float = 5.0  # whole batch floating loss >= x% of equity => hard close all legs
    adverse_fade_units: float = 99.0  # (disabled) old early adverse-fade exit; wrong-way cut + broker SL govern

    # --- M1 impulse detector (per-second order flow) ---
    impulse_enabled: bool = True
    impulse_flow_seconds: float = 5.0   # order-flow window (up vs down ticks)
    impulse_vel_atr: float = 0.10       # 3 s move >= x * M1 ATR
    impulse_min_pressure: float = 0.35  # (up - down) / (up + down) in the surge direction
    impulse_bar_atr: float = 0.20       # bar (or flow-window) move >= x * M1 ATR
    impulse_min_spread_mult: float = 1.0  # ... and >= x * spread
    impulse_max_bar_atr: float = 1.6    # do not chase a bar already this extended
    impulse_min_accel: float = 0.8      # last 3 s move >= 0.8 x previous 3 s (not decelerating)
    impulse_counter_min: float = 0.70   # counter-trend impulse needs this strength (single order)
    # Adaptive learning from real closed bursts (see learner.py)
    learning_enabled: bool = True
    learn_min_samples: int = 8          # trades per bucket before it can be disabled or sized up
    learn_half_life_hours: float = 36.0 # decay -> old results fade and losing buckets get re-explored
    learn_max_mult: float = 2.0         # max size multiplier for proven positive-expectancy buckets
    # --- broker SL management: tight initial SL -> breakeven+ -> trailing ---
    sl_spread_mult: float = 2.0         # initial SL >= x * spread
    sl_atr_frac: float = 0.5            # initial SL >= x * M1 ATR (micro-ATR)
    hard_close_mult: float = 1.3        # software backup stop at x * broker SL distance
    be_lock_spread_mult: float = 0.3    # BE+ lock = open + x * spread (small profit)
    trail_atr_frac: float = 0.25        # trailing distance >= x * M1 ATR
    trail_spread_mult: float = 1.0      # trailing distance >= x * spread
    sl_modify_min_interval: float = 1.0 # seconds between SL modifications of one position
    wrong_way_seconds: float = 6.0      # no follow-through within x s => cut
    wrong_way_follow_spread: float = 0.5  # 'follow-through' = executable P/L recovered to >= -x * spread
    wrong_way_loss_frac: float = 0.6    # unlocked and losing >= x * SL distance with momentum against => cut
    batch_tp_atr: float = 0.6           # short take-profit for the whole burst: x * M1 ATR (when ATR known)

    # --- burst batches (2-5 same-direction orders on strong momentum; never martingale/grid) ---
    batch_min: int = 2
    batch_max: int = 5
    batch_full_score: float = 0.90   # |score| at which the batch reaches batch_max
    batch_tp_units: float = 1.0      # close the whole batch at +x vol units (executable price, after spread)
    batch_max_hold_seconds: float = 120.0
    batch_cooldown_seconds: float = 2.0
    # --- higher-timeframe trend filter (M1/M5 bars) ---
    trend_min_strength: float = 0.25   # below = flat
    countertrend_min_score: float = 0.80  # counter-trend entry needs this |score|, single order, half risk
    trend_refresh_seconds: float = 60.0
    min_margin_level_pct: float = 300.0
    margin_use_max_pct: float = 30.0  # new order margin must be < x% of free margin

    # --- loop ---
    loop_interval: float = 0.1
    snapshot_interval: float = 1.0
    magic: int = 26112201
    comment_prefix: str = "RBurst"
    timezone: str = "Asia/Bangkok"
    stop_closes_positions: bool = True

    @classmethod
    def load(cls) -> "EngineConfig":
        cfg = cls()
        path = os.environ.get("RB_CONFIG_FILE")
        data = {}
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                data.update(json.load(fh))
        raw = os.environ.get("RB_CONFIG_JSON", "").strip()     # server-side overrides (e.g. Render env var)
        if raw:
            try:
                data.update(json.loads(raw))
            except ValueError:
                pass
        for env, key in (("RB_BROKER_SL", "broker_sl"), ("RB_EMERGENCY_SL", "emergency_sl"),
                         ("RB_DAILY_LOSS_PCT", "daily_loss_limit_pct"), ("RB_MAX_DD_PCT", "equity_drawdown_pct"),
                         ("RB_LEARNING_ENABLED", "learning_enabled")):
            if os.environ.get(env, "").strip():
                data[key] = os.environ[env]
        names = {f.name for f in fields(cls)}
        for k, v in data.items():
            if k in names:
                cur = getattr(cfg, k)
                if isinstance(cur, bool):
                    v = v if isinstance(v, bool) else str(v).strip().lower() in ("1", "true", "yes", "on")
                    setattr(cfg, k, v)
                else:
                    setattr(cfg, k, type(cur)(v))
        return cfg

    def public(self) -> dict:
        return asdict(self)
