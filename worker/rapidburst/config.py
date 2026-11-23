"""Engine / risk configuration (one engine: the Rapid Micro-Burst scalper).

All values are conservative defaults intended for a DEMO account.  They can be
overridden with a JSON file (``RB_CONFIG_FILE``) on the worker machine.  The
UI cannot change risk limits and can never enable LIVE trading.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict, fields


RISK_ENV = (("RB_BROKER_SL", "broker_sl"), ("RB_SAFE_PAUSE_S", "safe_pause_s"),
            ("RB_MAX_DD_PCT", "equity_drawdown_pct"), ("RB_LEARNING_ENABLED", "learning_enabled"),
            ("RB_RISK_PER_TRADE_PCT", "risk_per_trade_pct"), ("RB_MAX_TOTAL_RISK_PCT", "max_total_risk_pct"),
            ("RB_MAX_MARGIN_USE_PCT", "max_margin_use_pct"), ("RB_MAX_ORDERS_PER_GROUP", "max_orders_per_batch"),
            ("RB_MAX_ORDERS_PER_BATCH", "max_orders_per_batch"), ("RB_RISK_PER_BATCH_PCT", "risk_per_batch_pct"),
            ("RB_GROUP_LOSS_PCT", "group_loss_pct"), ("RB_MB_MAX_CHASE_ATR", "mb_max_chase_atr"),
            ("RB_MB_MAX_BAR_ATR", "mb_max_bar_atr"), ("RB_MB_THRESHOLD", "mb_threshold"),
            ("RB_SL_ATR", "sl_atr"), ("RB_COMMISSION_PER_LOT", "commission_per_lot"))
INFRA_ENV = {"RB_ALLOW_LIVE", "RB_BROKER", "RB_CLOUD_DATA_DIR", "RB_CONFIG_FILE", "RB_CONFIG_JSON", "RB_DATA_DIR",
             "RB_ENV_FILE", "RB_FALLBACK_POLL_MINUTES", "RB_KEEPALIVE_SECONDS", "RB_LINGER_SECONDS",
             "RB_STANDBY_SECONDS", "RB_TEST_BROKER_FACTORY", "RB_UI_PASSWORD", "RB_WAKE_URL"}


@dataclass
class EngineConfig:
    # --- tick data layer ---
    fast_window: float = 3.0          # base velocity window (s) for the volatility unit
    slow_window: float = 15.0
    vol_window: float = 120.0         # volatility normalisation window (s)
    breakout_lookback: float = 20.0
    min_ticks_warmup: int = 4         # ticks required before any decision (scale seeded from broker M1 candles)
    warmup_seconds: float = 2.5
    min_tick_rate: float = 0.3        # ticks/s below which the market is 'quiet'
    max_tick_age: float = 10.0        # older last tick => market idle, no decision

    # --- spread / execution quality gate ---
    max_spread_bps: float = 5.0       # spread cap relative to price (basis points of mid)
    max_spread_points: float = 0.0    # optional absolute cap in points (0 = off)
    max_spread_vol_ratio: float = 3.0 # spread must be < ratio * volatility unit
    spread_spike_ratio: float = 2.0   # spread > ratio * median spread => no entry
    max_avg_slippage_vu: float = 1.5  # recent fills' average slippage (vol units) above this => pause entries
    slippage_window_s: float = 90.0
    deviation_points: int = 20

    # --- Micro-Burst engine: analysis & dynamic strength (microburst.py) ---
    mb_vel_seconds: float = 1.5       # velocity window (s); acceleration = this window vs the previous one
    mb_flow_seconds: float = 3.0      # tick-direction / bid-ask proxy window (s)
    mb_struct_seconds: float = 20.0   # micro high/low window (s)
    mb_swing_seconds: float = 20.0    # swing used for anti-chase and burst identity (s)
    mb_vel_ref_atr: float = 0.25      # velocity (ATR per window) that counts as full strength
    mb_min_velocity: float = 0.35     # the chosen side needs at least this much live velocity component
    mb_threshold: float = 0.55        # base strength threshold (adapted to regime, spread and slippage risk)
    mb_edge: float = 0.12             # chosen side must beat the other side by this much
    mb_scan_support: float = 0.35     # a scanner (A/B/C) supports a side when its score >= this
    mb_max_chase_atr: float = 0.90    # RB_MB_MAX_CHASE_ATR: no entry if price is > x ATR from the recent swing
    mb_max_bar_atr: float = 1.00      # RB_MB_MAX_BAR_ATR: no entry if the live candle body is > x ATR in that direction
    mb_abnormal_range_atr: float = 1.5
    mb_wick_reject: float = 0.5       # wick >= x of the candle range = rejection
    mb_compress_ratio: float = 1.2
    mb_absorb_activity: float = 1.6   # activity >= x baseline while price stalls = absorption
    mb_max_gap_s: float = 5.0         # feed considered unstable above this gap
    mb_max_spread_target: float = 0.6 # spread must be < x * the capture target
    # exits (one basket mechanism)
    mb_target_atr: float = 0.20       # adaptive capture target = max(x * ATR * regime factor, mb_target_spread * spread)
    mb_target_spread: float = 2.0
    mb_min_net_spread: float = 0.3    # profit exits need >= x * spread of NET profit
    mb_full_target_mult: float = 2.0  # close at once at x * target
    mb_giveback: float = 0.40         # profit protection: close after giving back x of the peak (peak >= 0.6 target)
    mb_giveback_running: float = 0.60 # ... while momentum is still running (no close on tiny pullbacks)
    mb_flip_atr: float = 0.02         # tick flip = velocity against >= x ATR (or half a spread)
    mb_decay_frac: float = 0.30       # velocity below x * its peak since entry = decay
    mb_opp_pressure: float = 0.40     # opposing tick / bid-ask proxy >= x
    mb_stall_seconds: float = 3.0     # no new favourable extreme for x s = stall
    mb_follow_seconds: float = 4.0    # no follow-through within x s (scaled by regime) => cut
    mb_cut_frac: float = 0.50         # losing >= x of the stop distance with momentum against => cut
    mb_max_hold_seconds: float = 120.0  # stale guard: still not in net profit after x s => close (0 = off)
    commission_per_lot: float = 0.0   # round-trip commission per lot in account currency (cost in net P/L)

    # --- orders, batch & exposure (risk-based; no martingale, no averaging down, no grid) ---
    risk_per_trade_pct: float = 0.5   # RB_RISK_PER_TRADE_PCT: % of equity lost if one order's stop is hit
    sl_atr: float = 0.8               # hard stop distance = max(x * M1 ATR, sl_spread_mult * spread, stops level)
    sl_spread_mult: float = 4.0
    soft_stop_mult: float = 1.15      # engine mirror of the hard stop (if the broker SL fails)
    # burst batch: ONE batch per signal, opened at once, same lot per order, never added to later
    max_orders_per_batch: int = 3     # RB_MAX_ORDERS_PER_BATCH: orders in one batch (more only for stronger signals)
    batch_step_strength: float = 0.08 # +1 order per x strength above the threshold (up to the cap)
    risk_per_batch_pct: float = 1.0   # RB_RISK_PER_BATCH_PCT: whole batch loses <= x% equity if every stop is hit
    max_batch_lots: float = 1.0       # lots in one batch
    batch_run_cancel_frac: float = 0.5  # cancel the rest of a batch if price ran > x * target from the decision
    group_cooldown_seconds: float = 1.0
    max_lot_per_order: float = 1.0
    max_positions_total: int = 6
    max_positions_per_symbol: int = 3
    max_total_lots: float = 2.0
    max_total_notional_x: float = 500.0  # sum of open notional <= x * equity
    max_total_risk_pct: float = 3.0   # RB_MAX_TOTAL_RISK_PCT: sum of open stop risk <= x% of equity
    max_margin_use_pct: float = 50.0  # RB_MAX_MARGIN_USE_PCT: total used margin <= x% of equity
    min_margin_level_pct: float = 200.0
    margin_use_max_pct: float = 50.0  # one order may use <= x% of the free margin
    small_account_max_loss_pct: float = 0.0  # 0 = min lot always allowed (free margin permitting)
    group_loss_pct: float = 2.0       # RB_GROUP_LOSS_PCT: hard exit when one basket's floating loss >= x% equity
    # NO all-day daily-loss halt. Equity drawdown => SAFE MODE (flatten, pause new entries, resume reduced)
    equity_drawdown_pct: float = 8.0  # RB_MAX_DD_PCT: equity x% below the session peak => safe mode (0 = off)
    safe_pause_s: float = 300.0       # first safe-mode pause; doubles on each repeat in a session ...
    safe_pause_max_s: float = 1800.0  # ... up to this
    safe_probe_batches: int = 3       # after a pause: this many single min-lot batches before normal sizing
    emergency_stale_s: float = 15.0   # open positions and no fresh price for x s => emergency close
    exec_fail_max: int = 3            # entry rejects within 60 s before the symbol is paused
    exec_fail_pause_s: float = 60.0
    close_fail_halt: int = 5          # close attempts failing this many times => halt entries

    # --- broker stop management ---
    broker_sl: bool = True            # real broker SL on every order (RB_BROKER_SL=0 to disable)
    be_lock_spread_mult: float = 0.3  # BE+ lock = open + x * spread
    trail_atr_frac: float = 0.15      # trailing hard stop distance >= x * M1 ATR
    trail_spread_mult: float = 1.0
    sl_modify_min_interval: float = 1.0

    # --- entry pacing ---
    cooldown_seconds: float = 1.0
    loss_cooldown_seconds: float = 2.0
    max_entries_per_minute: int = 30
    max_consecutive_losses: int = 4
    loss_pause_seconds: float = 30.0

    # --- learning from real closed groups ---
    learning_enabled: bool = True
    learn_min_samples: int = 8
    learn_half_life_hours: float = 36.0
    learn_max_mult: float = 1.5

    # --- context ---
    trend_min_strength: float = 0.25   # M1/M5 trend: display/context only, NOT an entry gate
    trend_refresh_seconds: float = 30.0

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
        # (RB_DAILY_LOSS_PCT is no longer read: there is no all-day daily-loss halt)
        for env, key in RISK_ENV:
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

    @classmethod
    def env_report(cls) -> dict:
        """Which risk/engine env overrides are active on this server (values of these numeric settings only),
        RB_CONFIG_JSON keys (no values) and NAMES of set RB_* variables this version ignores (never values)."""
        names = {f.name for f in fields(cls)}
        applied = {e: os.environ[e].strip() for e, _ in RISK_ENV if os.environ.get(e, "").strip()}
        cj = []
        raw = os.environ.get("RB_CONFIG_JSON", "").strip()
        if raw:
            try:
                cj = sorted(json.loads(raw))
            except ValueError:
                cj = ["<invalid JSON>"]
        known = {e for e, _ in RISK_ENV} | INFRA_ENV
        ignored = sorted(k for k in os.environ if k.startswith("RB_") and k not in known)
        return {"applied": applied, "config_json_keys": cj,
                "config_json_ignored": [k for k in cj if k not in names and k != "<invalid JSON>"],
                "ignored_env": ignored}

    def public(self) -> dict:
        return asdict(self)
