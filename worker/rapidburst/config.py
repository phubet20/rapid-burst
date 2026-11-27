"""Engine / risk configuration (one engine: the M1 Rapid Basket Scalper).

Any MT5 account (demo or real, any broker) can be connected from the UI (t46u, t49u: no lock at all); formerly RB_ALLOW_LIVE=0 locked
REAL accounts out. t30u: the owner asked to remove the risk-amount limits so the account can compound as fast as
possible; the defaults below are therefore AGGRESSIVE (half the equity at risk per basket if every stop is hit,
safe mode only at a 50 % drawdown). Every order still carries a hard broker stop.  They can be
overridden with a JSON file (``RB_CONFIG_FILE``) on the worker machine.  The
UI cannot change risk limits.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict, fields


RISK_ENV = (("RB_SAFE_PAUSE_S", "safe_pause_s"),
            ("RB_MAX_DD_PCT", "equity_drawdown_pct"), ("RB_LEARNING_ENABLED", "learning_enabled"),
            ("RB_RISK_PER_TRADE_PCT", "risk_per_trade_pct"), ("RB_MAX_TOTAL_RISK_PCT", "max_total_risk_pct"),
            ("RB_MAX_MARGIN_USE_PCT", "max_margin_use_pct"), ("RB_MAX_ORDERS_PER_GROUP", "max_orders_per_batch"),
            ("RB_MAX_ORDERS_PER_BATCH", "max_orders_per_batch"), ("RB_RISK_PER_BATCH_PCT", "risk_per_batch_pct"),
            ("RB_GROUP_LOSS_PCT", "group_loss_pct"), ("RB_MB_MAX_CHASE_ATR", "mb_max_chase_atr"),
            ("RB_MB_MAX_BAR_ATR", "mb_max_bar_atr"), ("RB_MB_THRESHOLD", "mb_threshold"),
            ("RB_SL_ATR", "sl_atr"), ("RB_COMMISSION_PER_LOT", "commission_per_lot"))
def allow_live_from_env(env=None) -> bool:
    """t49u: NO account lock of any kind - demo, contest and real accounts of any broker/server connect and trade.
    (The old RB_ALLOW_LIVE=0 server switch was removed on the owner's request; the variable is ignored.)"""
    return True


INFRA_ENV = {"RB_ALLOW_LIVE", "RB_BROKER", "RB_CLOUD_DATA_DIR", "RB_CONFIG_FILE", "RB_CONFIG_JSON", "RB_DATA_DIR",
             "RB_ENV_FILE", "RB_FALLBACK_POLL_MINUTES", "RB_LEARN_REDIS_URL", "RB_KEEPALIVE_SECONDS", "RB_LINGER_SECONDS",
             "RB_STANDBY_SECONDS", "RB_TEST_BROKER_FACTORY", "RB_UI_PASSWORD", "RB_WAKE_URL"}


# Engine settings that may be changed at RUNTIME (no redeploy, the MT5 login stays): name -> (min, max).
# Only strategy/sizing knobs inside safe bounds. Never: live trading, broker SL on/off, magic, credentials.
TUNABLE = {
    "sl_struct_buffer_atr": (0.0, 0.5), "sl_min_atr": (0.2, 2.0), "sl_max_atr": (0.5, 4.0),
    "ind_min_score": (0.0, 0.6), "ind_sr_near_atr": (0.0, 1.0), "safe_flatten_pct": (0.0, 95.0), "exit_min_r": (0.0, 2.0), "reverse_enabled": (0, 1), "reverse_edge": (0.0, 0.5),
    "nostop_margin_use_pct": (5.0, 90.0), "batch_margin_pct": (1.0, 60.0), "emergency_margin_level_pct": (50.0, 1000.0),
    "zone_target_exit": (0, 1), "full_batch": (0, 1),
    "regime_enabled": (0, 1), "regime_pivot_k": (1, 5), "regime_max_phase": (3, 12), "regime_adx_min": (0.0, 40.0),
    "regime_chase_atr": (0.5, 6.0), "regime_bonus": (0.0, 0.3), "regime_counter_veto": (0, 1), "regime_mid_penalty": (0.0, 0.3),
    # t44u: hedge / averaging recovery is CANCELLED (owner) - hedge_enabled, avg_*, recovery_* are deliberately NOT tunable
    # and EngineConfig.load() forces them off, so neither the UI nor RB_CONFIG_JSON can switch them back on
    "cycle_tp_pct": (0.01, 10.0), "tp_min_money": (0.0, 100.0),
    "tp_target_atr": (0.05, 3.0), "tp_target_money": (0.0, 1000.0), "tp_target_giveback": (0.05, 0.9),
    "tp_grab_x": (1.0, 5.0), "profit_lock_min_money": (0.0, 100.0), "profit_lock_spread_x": (0.5, 10.0), "profit_lock_giveback": (0.1, 0.9),
    "profit_lock_floor_frac": (0.0, 0.9), "profit_lock_confirm_s": (0.0, 3.0), "zones_enabled": (0, 1), "zone_weight": (0.0, 0.3), "zone_impulse_atr": (0.5, 3.0),
    "pullback_hold": (0, 1), "exit_confirm_s": (0.0, 5.0), "exit_confirm_ticks": (1, 20), "exit_outlier_atr": (0.2, 5.0), "exit_stale_s": (0.5, 10.0),
    "candle_poll_m5_s": (5, 120),
    "ind_enabled": (0, 1), "ind_chop_veto": (0, 1), "mb_require_m1_trigger": (0, 1), "sl_structure": (0, 1),
    "mb_threshold": (0.35, 0.90), "mb_edge": (0.0, 0.40), "mb_min_agree": (2, 5), "mb_scan_support": (0.2, 0.7),
    "mb_max_chase_atr": (0.3, 4.0), "mb_max_bar_atr": (0.3, 5.0), "loss_streak_threshold_add": (0.0, 0.1),
    "mb_min_velocity": (0.05, 0.8), "m1_trend_veto": (0.2, 1.0), "min_orders_per_batch": (1, 8),
    "precision_enabled": (0, 1), "pe_min": (0.3, 0.9), "pe_dir_relax": (0.0, 0.3), "pe_rr_min": (1.0, 4.0), "pe_min_depth": (0.1, 0.5), "pe_zone_chase_atr": (0.1, 1.0),
    "pe_max_sl_atr": (0.5, 3.0), "pe_min_confluence": (2, 4), "pe_range_prem_max": (0.6, 1.0), "pe_breakout_enabled": (0, 1), "pe_bo_cont_ext_atr": (0.2, 2.0), "pe_bo_max_bars": (2, 20), "pe_max_cost_frac": (0.05, 0.6), "pe_tgt_min_atr": (0.3, 3.0), "pe_disp_atr": (0.8, 3.0),
    "add_enabled": (0, 1), "add_pullback_atr": (0.03, 1.0), "add_max_per_step": (1, 8),
    "m1_only": (0, 1), "m1_every_bar": (0, 1), "hold_winners": (0, 1),
    "scalp_enabled": (0, 1), "scalp_be_atr": (0.08, 0.6), "scalp_trail_mult": (0.3, 1.0), "scalp_target_atr": (0.15, 2.0),
    "scalp_partial_frac": (0.25, 0.9), "scalp_giveback_min_atr": (0.05, 1.0), "scalp_giveback_frac": (0.2, 0.8),
    "sniper_enabled": (0, 1), "sniper_min_wick": (0.1, 0.6), "sniper_min_score": (0.3, 0.9), "sniper_chase_atr": (0.2, 1.5),
    "ip_enabled": (0, 1), "ip_m5_bias_min": (0.0, 0.8), "ip_impulse_atr": (1.0, 6.0), "ip_eff_min": (0.2, 0.9),
    "ip_retr_min": (0.15, 0.6), "ip_retr_max": (0.4, 0.85), "ip_fail_retr": (0.6, 1.0), "ip_resume_atr": (0.05, 1.0),
    "ip_be_atr": (0.2, 3.0), "ip_trail_atr": (0.3, 4.0), "ip_m5_flip": (0.1, 0.9), "ip_stale_s": (20.0, 900.0),
    "alloc_enabled": (0, 1), "alloc_margin_pct": (5.0, 90.0),
    "avoid_foreign_conflict": (0, 1), "ip_single_owner": (0, 1),
    "shadow_enabled": (0, 1), "shadow_confirm_s": (2.0, 30.0), "shadow_wick": (0.2, 0.7), "shadow_step_atr": (0.05, 0.6),
    "shadow_levels": (1, 10), "shadow_sl_atr": (0.4, 3.0), "shadow_max_spread_atr": (0.05, 1.0), "shadow_min_n": (20, 2000),
    "entries_enabled": (0, 1), "tpl_enabled": (0, 1), "tpl_require_evidence": (0, 1), "tpl_max_hold_s": (30.0, 600.0),
    "tpl_flip_imb": (0.1, 0.9),
    "compound_enabled": (0, 1), "compound_step": (0.05, 1.0), "compound_max_mult": (1.0, 4.0),
    "ladder_enabled": (0, 1), "ladder_first_frac": (0.25, 1.0), "ladder_step_atr": (0.15, 1.5), "ladder_levels": (1, 4), "ladder_ttl_s": (30, 1800), "hold_flip_bias": (0.1, 0.8),
    "exit_spike_atr": (0.1, 3.0), "exit_spike_wick_frac": (0.1, 0.9), "candle_poll_m1_s": (3, 60),
    "learn_max_mult": (1.0, 3.0), "learn_min_samples": (2, 50), "max_positions_total": (1, 200), "max_positions_per_symbol": (1, 100),
    "max_lot_per_order": (0.01, 100.0), "max_batch_lots": (0.01, 500.0), "max_total_lots": (0.01, 1000.0),
    "min_margin_level_pct": (50.0, 1000.0), "margin_use_max_pct": (10.0, 100.0), "safe_probe_batches": (0, 5),
    "exit_min_signals": (2, 6), "exit_retrace_atr": (0.05, 1.0), "exit_retrace_spread": (0.5, 6.0),
    "exit_retrace_frac": (0.1, 0.9), "exit_opp_frac": (0.5, 1.5), "mb_cut_frac": (0.0, 1.0),
    "mb_max_hold_seconds": (0, 1800), "mb_min_net_spread": (0.0, 3.0),
    "be_trigger_atr": (0.1, 2.0), "be_trigger_spread": (1.0, 10.0), "be_lock_spread_mult": (0.0, 2.0),
    "trail_atr_frac": (0.05, 1.5), "trail_atr_strong": (0.05, 2.0), "trail_atr_fade": (0.03, 1.0),
    "trail_spread_mult": (1.0, 6.0), "sl_atr": (0.3, 3.0), "sl_spread_mult": (2.0, 12.0),
    "max_orders_per_batch": (1, 40), "risk_per_trade_pct": (0.1, 100.0), "risk_per_batch_pct": (0.2, 100.0),
    "small_account_batch_risk_pct": (2.0, 100.0), "max_total_risk_pct": (1.0, 300.0),
    "equity_drawdown_pct": (0.0, 90.0), "safe_pause_s": (30, 3600), "safe_min_stopouts": (1.0, 3.0),
    "safe_dd_cap_pct": (10.0, 90.0), "group_loss_pct": (0.5, 100.0), "max_margin_use_pct": (10.0, 95.0),
    "learning_enabled": (0, 1),
}


def validate_tune(values) -> tuple[dict, list]:
    """(accepted {name: typed value}, rejected [reasons]) for a runtime tuning request."""
    ok, bad = {}, []
    if not isinstance(values, dict):
        return ok, ["values must be an object"]
    defaults = EngineConfig()
    for k, v in list(values.items())[:40]:
        k = str(k)[:60]
        if k not in TUNABLE:
            bad.append(f"{k}: not tunable at runtime")
            continue
        lo, hi = TUNABLE[k]
        cur = getattr(defaults, k)
        try:
            x = float(v)
        except (TypeError, ValueError):
            bad.append(f"{k}: not a number")
            continue
        if x != x or not (lo <= x <= hi):
            bad.append(f"{k}: {v} outside [{lo}, {hi}]")
            continue
        ok[k] = bool(x) if isinstance(cur, bool) else int(round(x)) if isinstance(cur, int) else x
    return ok, bad


@dataclass
class EngineConfig:
    # --- tick data layer ---
    fast_window: float = 3.0          # base velocity window (s) for the volatility unit
    slow_window: float = 15.0
    vol_window: float = 120.0         # volatility normalisation window (s)
    breakout_lookback: float = 20.0
    min_ticks_warmup: int = 3         # ticks required before any decision (scale seeded from broker M1 candles)
    warmup_seconds: float = 1.0
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
    mb_min_velocity: float = 0.08     # the chosen side needs at least this much live velocity component
    mb_threshold: float = 0.40        # t35u: 0.55 -> 0.40 (live: 'below threshold' was ~50/60 s of every minute); base strength threshold (adapted to regime, spread and slippage risk)
    mb_edge: float = 0.06             # t35u: 0.08 -> 0.03, t44u: 0.06 (no hedge safety net any more: a near-tie BUY/SELL is noise); chosen side must beat the other side by this much (each side scored on its own)
    mb_scan_support: float = 0.35     # a family (A/B/C/D/E) supports a side when its score >= this
    mb_min_agree: int = 2             # families that must agree (of A tick, B activity/candle, C structure,
                                      # D M1 timing, E M5 bias), one of them C or D (structure / candle trigger):
                                      # speed+volume alone (A+B) never trades
    candle_poll_m5_s: float = 20.0    # t34u: broker M5 candles (main momentum frame; forming bar = live ticks)
    candle_poll_m1_s: float = 8.0     # broker M1 candle refresh (the forming bar comes from live ticks)
    mb_max_chase_atr: float = 1.80    # RB_MB_MAX_CHASE_ATR: no entry if price is > x ATR from the recent swing
    mb_max_bar_atr: float = 2.20      # RB_MB_MAX_BAR_ATR: no entry if the live candle body is > x ATR in that direction
    mb_abnormal_range_atr: float = 1.5
    mb_wick_reject: float = 0.5       # wick >= x of the candle range = rejection
    mb_compress_ratio: float = 1.2
    mb_absorb_activity: float = 1.6   # activity >= x baseline while price stalls = absorption
    mb_max_gap_s: float = 5.0         # feed considered unstable above this gap
    mb_max_spread_target: float = 0.6 # spread must be < x * the capture target
    # expected short move (NOT a take-profit: used for the spread/cost gate and anti-chase only)
    mb_target_atr: float = 0.20       # = max(x * ATR * regime factor, mb_target_spread * spread)
    mb_target_spread: float = 2.0
    # exits: momentum hold + combined exhaustion (one basket mechanism, no fixed TP)
    mb_min_net_spread: float = 0.3    # "in profit" = NET basket P/L >= x * spread
    exit_min_signals: int = 3         # exhaustion signals that must agree to close a basket in profit
    exit_rsi_turn: float = 4.0        # M1 RSI points per bar against the basket = RSI momentum change
    exit_opp_frac: float = 0.9        # opposite-side strength >= x * threshold = directional reversal
    exit_retrace_atr: float = 0.25    # meaningful retracement from the basket peak >= max(x * ATR, ...
    exit_retrace_spread: float = 2.0  # ... x * spread) and >= exit_retrace_frac of the peak (noise-aware)
    exit_retrace_frac: float = 0.45
    mb_flip_atr: float = 0.02         # tick flip = velocity against >= x ATR (or half a spread)
    mb_decay_frac: float = 0.30       # velocity below x * its peak since entry = decay
    mb_opp_pressure: float = 0.40     # opposing tick / bid-ask proxy >= x
    mb_stall_seconds: float = 3.0     # no new favourable extreme for x s = stall
    mb_cut_frac: float = 0.0          # losing >= x of the stop distance with >= 2 reversal signals => cut
    exit_spike_atr: float = 0.5       # spike exit: peak net profit >= x * M1 ATR and the bar turns back ...
    exit_spike_wick_frac: float = 0.3 # ... (retraced >= x of the peak + velocity flip + opposing pressure/wick)
    m1_trend_veto: float = 0.45       # M1 main trend this strongly against = no counter-trend entry (unless the
                                      # M1 / micro structure just broke in the entry direction)
    mb_max_hold_seconds: float = 0.0  # stale guard: not in net profit after x s AND momentum gone => close
    commission_per_lot: float = 0.0   # round-trip commission per lot in account currency (cost in net P/L)

    # --- orders, batch & exposure (risk-based; no martingale, no averaging down, no grid) ---
    risk_per_trade_pct: float = 10.0  # RB_RISK_PER_TRADE_PCT: % of equity lost if one order's stop is hit
    sl_atr: float = 0.8               # hard stop distance = max(x * M1 ATR, sl_spread_mult * spread, stops level)
    sl_spread_mult: float = 4.0
    # t32u STRUCTURE stop: beyond the last M1 swing low (BUY) / high (SELL) + spread + sl_struct_buffer_atr*ATR,
    # clamped to [sl_min_atr, sl_max_atr] x ATR (and >= stops level, >= 3x spread); ATR stop when no swing
    sl_structure: bool = True
    sl_struct_buffer_atr: float = 0.10
    sl_min_atr: float = 0.5
    sl_max_atr: float = 1.6
    # t32u indicator confluence (family F) - indicators.py
    ind_enabled: bool = True
    ind_min_score: float = 0.30       # confluence for the side below this (indicators lean against) => veto
    ind_sr_near_atr: float = 0.20     # no entry into S/R closer than x ATR (unless breaking it)
    ind_chop_veto: bool = False        # ADX < 18 with no breakout, or a squeeze without the break => no entry
    reverse_enabled: bool = True      # t34u AUTO-REVERSAL: opposite side has the edge -> close, verify, open opposite
    # t36u: anti-jitter for SOFT exits (reversal, spike-turn, momentum, structure, profit protection, auto-reverse):
    # the exit signal must persist >= exit_confirm_s AND over >= exit_confirm_ticks distinct ticks on clean data
    # (no feed gap / stale quote, no spread spike, no single outlier tick); hard stop / broker SL are never delayed
    # t37u: full batch from the first entry (capped only by stop-risk / margin / exposure), more orders per basket,
    # price-action context and pullback hold
    # t38u (OWNER REQUEST, demo, risk accepted): NO per-order stop loss. One full batch at the signal sized by
    # MARGIN (nostop_margin_use_pct of equity, buffer kept so orders are not rejected); exits by zone target or a
    # confirmed pullback/reversal (volume + momentum + indicators, anti-jitter). The ONLY emergency guards are the
    # margin level (< emergency_margin_level_pct closes the bot's positions) and data-feed loss. BE/trail on winners
    # stays as engine-side profit protection. Set no_stop_mode=0 to restore stop-based sizing + broker SL.
    no_stop_mode: bool = False        # t45u: OFF - every order has a structural broker SL (the precision spec requires it);
                                      # load() keeps it off while precision_enabled (not tunable, not in the env overrides)
    nostop_margin_use_pct: float = 75.0     # t41u: TOTAL margin of all bot positions (batches+hedge+layers) <= x%
    batch_margin_pct: float = 10.0          # t40u: margin of ONE batch / hedge / recovery layer <= x% of equity
    # t40u market state: UP / DOWN / SIDEWAYS + trend phase counter (M5 forming bars, swings, ADX)
    regime_enabled: bool = True
    regime_pivot_k: int = 2
    regime_max_phase: int = 5
    regime_adx_min: float = 15.0
    regime_chase_atr: float = 1.5     # t44u: 2.0 -> 1.5; no entry once price is > x ATR (M5) beyond the pullback origin (already spiked)
    regime_bonus: float = 0.10        # strength bonus for a pullback-wick entry with the trend / a range-edge fade
    # t44u: an entry AGAINST an intact M5 structure (SELL while the state is UP, BUY while it is DOWN) is vetoed unless
    # a wick / demand-supply zone backs it (a real reversal breaks the structure first and flips the state, so it is
    # not blocked); the middle of a sideways range costs regime_mid_penalty (was 0.06)
    regime_counter_veto: bool = True
    regime_mid_penalty: float = 0.10
    # t40u recovery (OWNER-REQUESTED) was CANCELLED in t44u (owner): the hedge basket and the averaging layers kept
    # opening opposite / extra baskets on every red basket, leaving one side green and the other red. Both stay OFF
    # (EngineConfig.load() forces them off; they are not runtime-tunable). The fields remain only so old saved state,
    # tests and the legacy CYCLE TAKE PROFIT path (closing already-open mixed sets together) keep working.
    hedge_enabled: bool = False
    recovery_after_s: float = 20.0
    recovery_loss_pct: float = 2.0
    avg_enabled: bool = False
    avg_spacing_atr: float = 1.0      # each layer >= x M1 ATR beyond the previous worst entry
    avg_max_layers: int = 4
    avg_lot_mult: float = 1.0         # lot multiplier per layer (1.0 = no martingale; tunable up by the owner)
    cycle_tp_pct: float = 0.3         # a mixed / layered set closes TOGETHER when the symbol's combined net >= x% of equity
    tp_min_money: float = 0.0         # close only when the basket net > this (account currency)
    # t41u fast PROFIT LOCK: once a basket's net > max(profit_lock_min_money, profit_lock_spread_x x its spread cost)
    # the profit is locked; it closes at once (short anti-jitter confirm) when the net gives back
    # profit_lock_giveback of its peak or falls to the floor (profit_lock_floor_frac x the lock level) - before red
    # t43u profit TARGET (not tiny): basket net >= max(tp_target_money, tp_target_atr x M1 ATR on its lots); after it
    # the profit is grabbed when it gives back tp_target_giveback of its peak, or at once at tp_grab_x x the target
    # t44u (owner: closes were too short): target 0.4 -> 0.6 ATR, immediate grab 1.5x -> 2.0x the target (0.6 -> 1.2 ATR),
    # giveback 20% -> 25% of the peak so a normal wobble no longer ends a basket that is still running
    tp_target_atr: float = 0.6
    tp_target_money: float = 0.10
    tp_target_giveback: float = 0.25
    tp_grab_x: float = 2.0
    profit_lock_min_money: float = 0.03
    profit_lock_spread_x: float = 2.0
    profit_lock_giveback: float = 0.4
    profit_lock_floor_frac: float = 0.25
    profit_lock_confirm_s: float = 0.3
    emergency_margin_level_pct: float = 110.0   # t41u: < 1/0.75 = 133% so a full 75% load does not trip it
    zone_target_exit: bool = True     # take profit when price reaches an opposing zone / FVG / S-R level
    full_batch: bool = True
    zones_enabled: bool = True
    zone_weight: float = 0.10         # max +-0.10 strength from DM/SP zones, FVG, confirmation candle, wick tip
    zone_impulse_atr: float = 1.2     # impulsive candle body >= x ATR marks the base before it as a zone
    pullback_hold: bool = True        # pullback into the basket's own demand/supply zone or FVG does not close it
    exit_confirm_s: float = 0.8
    exit_confirm_ticks: int = 2
    exit_outlier_atr: float = 0.8     # a tick > x ATR away from the median of the last 5 mids is an outlier
    exit_stale_s: float = 2.0         # last tick older than this = stale data, never a reversal
    reverse_edge: float = 0.10        # opposite strength must beat the held side's strength by this much
    exit_min_r: float = 0.0           # discretionary profit exits only at net >= x * stop distance (0 = off)
    mb_require_m1_trigger: bool = False  # once M1 bars exist, the M1 trigger family D must agree
    safe_flatten_pct: float = 60.0    # last resort: equity this % below the closed-balance peak => flatten all
    soft_stop_mult: float = 1.15      # engine mirror of the hard stop (if the broker SL fails)
    # batch: ONE batch per signal, opened at once, same lot per order, never added to later
    max_orders_per_batch: int = 16    # RB_MAX_ORDERS_PER_BATCH: orders in one batch (a CAP, never forced: the count
                                      # follows momentum quality, then equity/lot/margin/exposure limits cut it)
    # order count from the score: n = 1 + round(7 x q), q = (strength - threshold) / (1 - threshold), then cut by
    # risk, margin, stop distance and exposure
    risk_per_batch_pct: float = 10.0  # RB_RISK_PER_BATCH_PCT: whole batch loses <= x% equity if every stop is hit
    max_batch_lots: float = 100.0      # lots in one batch
    batch_run_cancel_frac: float = 1.0  # cancel the rest of a batch if price ran > x * target from the decision
    # t45u STRUCTURE-TO-LIQUIDITY PRECISION ENTRY (precision.py): pullback entry from M5 structure (BOS/CHOCH +
    # displacement), OB, liquidity targets, fibo as ONE confluence factor, trigger on the live M1, structural stop,
    # reward-to-risk to the Target LIQ. The Position Entry Score is separate from the directional momentum score.
    precision_enabled: bool = True
    # t46u (owner): FAST M1 TREND MODE - entries are scored on M1 only (EMA / MACD / RSI / tick force; the M5 family and the
    # M5 structure / regime vetoes and the M5 precision plan are not used), trade WITH the M1 trend, add on shallow pullbacks
    # that resume, re-enter at once after a close. While on, precision_enabled is forced off and every order keeps its broker SL.
    m1_only: bool = True
    # t46u (owner): HOLD WINNERS - a basket in net profit is not closed by the discretionary exits (momentum exhausted, spike,
    # profit protection, zone target, auto-reversal) unless the M1 trend has really turned against it; while the whole bot
    # portfolio is still negative a winner whose stop is already at break-even / trailing is held to that stop. Hard risk exits,
    # the break-even / trailing stop and the broker SL are untouched.
    hold_winners: bool = True
    # t46u (owner): ENTER ON EVERY M1 BAR - no threshold / agreement / trigger / anti-chase waiting: each new M1 bar opens one
    # basket in the M1 trend direction (EMA / MACD / RSI bias; the stronger side when flat). Only valid with m1_only. Still
    # applies: clean feed, spread / data checks, broker SL on every order, the risk / lot / margin / exposure caps, no hedge
    # and no averaging down (a second basket is only stacked onto a symbol whose open baskets are all the same side and in profit).
    m1_every_bar: bool = False
    # t46u (owner): PULLBACK LADDER - a new basket sends only `ladder_first_frac` of its orders at market; the rest wait for price
    # to pull back `ladder_step_atr` x M1 ATR (x1, x2 ... `ladder_levels` steps) and are then sent at market, SAME lot, SAME structural
    # stop (so each later order risks LESS than the first), total orders / lots / risk = the one batch budget (no martingale, no
    # lot growth). Dropped when the M1 trend turns against the basket, the stop is too close, the basket reaches break-even / trail,
    # or after `ladder_ttl_s`. Needs a broker SL; never used in no-stop mode.
    ladder_enabled: bool = True
    ladder_first_frac: float = 0.5
    ladder_step_atr: float = 0.35
    ladder_levels: int = 2
    ladder_ttl_s: float = 240.0
    # t47u (owner): SCALP - take short and lock fast. (1) the break-even lock arms at `scalp_be_atr` x M1 ATR of net basket profit
    # (instead of be_trigger_atr) and the trail is `scalp_trail_mult` x tighter; (2) PARTIAL TAKE-PROFIT: at `scalp_target_atr` x ATR
    # of net profit `scalp_partial_frac` of the basket's orders are closed in profit and the stop of the rest goes to break-even+ at
    # once (the runner); (3) GIVEBACK GUARD: once the basket peaked >= `scalp_giveback_min_atr` x ATR (and no break-even stop is in
    # place yet) it is closed in profit when it gives back `scalp_giveback_frac` of the peak - a winner is never allowed to run
    # back into a loss, and hold_winners never overrides it.
    scalp_enabled: bool = False       # t48u: OFF - let the profit run (no partial TP / giveback / tight trail)
    scalp_be_atr: float = 0.20
    scalp_trail_mult: float = 0.70
    scalp_target_atr: float = 0.55
    scalp_partial_frac: float = 0.50
    scalp_giveback_min_atr: float = 0.14
    scalp_giveback_frac: float = 0.45
    # t47u (owner): M1 WICK-REJECTION SNIPER entry (needs m1_only). The entry fires at the open of an M1 bar (or inside the bar once the
    # pullback wick has formed): BUY after a LOWER wick rejection, SELL after an UPPER wick rejection, price not chasing the move.
    # score = 0.40 wick + 0.20 close position + 0.25 trend alignment + 0.15 (1 - chase); needs wick >= `sniper_min_wick` (fraction of
    # the bar range), score >= `sniper_min_score` and the forming bar's move with the trade < `sniper_chase_atr` x average M1 range.
    sniper_enabled: bool = False      # t48u: replaced by the impulse -> pullback entry
    sniper_min_wick: float = 0.30
    sniper_min_score: float = 0.50
    sniper_chase_atr: float = 0.60
    # t47u (owner): COMPOUND the lot after wins (anti-martingale): lot x (1 + compound_step x consecutive winning baskets on that
    # symbol), capped at compound_max_mult; any losing basket resets it. Every risk / lot / margin / exposure cap still applies and
    # the stop-risk budget of the batch is never exceeded (the order count shrinks instead).
    # t48u (owner): IMPULSE -> PULLBACK entry (needs m1_only). Direction = M5 trend (EMA / MACD / HH-HL swing structure bias
    # >= `ip_m5_bias_min`); the entry = M1 only and NEVER at the bar open / on the way up: (1) IMPULSE = a leg of >= `ip_impulse_atr`
    # average M1 ranges in <= 8 bars with efficiency (net / path) >= `ip_eff_min`, with the M5 trend; (2) PULLBACK = price retraces
    # `ip_retr_min`..`ip_retr_max` of that leg and has turned back by >= `ip_resume_atr` ranges from the pullback extreme. A pullback
    # deeper than `ip_fail_retr` = the impulse failed (no entry). One entry per pullback. The stop is beyond the pullback extreme.
    # Exits: a loss leaves fast (stop beyond the pullback, impulse failed, no follow-through after `ip_stale_s`), a winner is held
    # (break-even only after `ip_be_atr` ATR, trail `ip_trail_atr` ATR wide - no stop hugging the price) until the M5 trend turns
    # (bias <= -`ip_m5_flip`) or the trail is hit.
    ip_enabled: bool = True
    # t49u anti-collision: (1) never open against a position that is NOT the bot's (another EA / manual trade on the same
    # symbol, opposite side); (2) an impulse-pullback basket has ONE owner - only its own exits (hard stop, protect/trail,
    # _ip_exit) close it, the older soft exit families (micro/zone/reversal) never fight it
    avoid_foreign_conflict: bool = True
    # t50u SHADOW LAB (rule hit-rates measured on the live feed, no orders) and the TP-LADDER entry that trades only what the lab
    # has proven. entries_enabled=0 = "shadow only": START runs the feed + lab but never opens a position.
    entries_enabled: bool = True
    shadow_enabled: bool = True
    shadow_confirm_s: float = 6.0          # evaluate this many seconds after the M1 bar opened
    shadow_wick: float = 0.35              # min wick / range of the last closed bar for the wick rule
    shadow_step_atr: float = 0.15          # take-profit step of ladder leg k = k * step (ATR of M1), net of the spread
    shadow_levels: int = 5                 # legs of the ladder
    shadow_sl_atr: float = 1.0             # the ladder's shared stop (ATR) behind the entry
    shadow_max_spread_atr: float = 0.35    # spread above this share of ATR: no observation / no entry (cost eats the target)
    shadow_min_n: int = 100                # observations before a verdict other than "collecting"
    tpl_enabled: bool = False              # LIVE TP-ladder entries (off by default)
    tpl_rule: str = "combo"                # which lab rule triggers it (wick/flow/mom/loc/combo)
    tpl_require_evidence: bool = True      # trade only while the lab shows this rule 'positive' (n>=min_n, ladder>0, z>=2)
    tpl_max_hold_s: float = 180.0          # what is still open leaves after this long (matches the lab's ladder window)
    tpl_flip_imb: float = 0.35             # ... or when the tick imbalance turns this far against the basket
    ip_single_owner: bool = True
    ip_m5_bias_min: float = 0.15
    ip_impulse_atr: float = 2.2
    ip_eff_min: float = 0.45
    ip_retr_min: float = 0.30
    ip_retr_max: float = 0.70
    ip_fail_retr: float = 0.80
    ip_resume_atr: float = 0.25
    ip_be_atr: float = 0.90
    ip_trail_atr: float = 1.20
    ip_m5_flip: float = 0.30
    ip_stale_s: float = 150.0
    # t48u (owner): lot from the PORTFOLIO - one basket may use `alloc_margin_pct` of equity as margin, split into equal orders
    # (the ladder spreads them over the pullback). The loss if every stop is hit stays capped by risk_per_batch_pct, the shared
    # max_margin_use_pct / max_total_risk_pct caps still apply, and the lot grows with equity after wins (no streak multiplier).
    alloc_enabled: bool = True
    alloc_margin_pct: float = 40.0
    compound_enabled: bool = False    # t48u: the lot now compounds with equity (alloc_margin_pct of equity), no streak multiplier
    compound_step: float = 0.25
    compound_max_mult: float = 2.0
    hold_flip_bias: float = 0.25      # M1 trend bias against the basket (with RSI on the other side of 50) = the trend turned
    pe_min: float = 0.58              # Position Entry Score needed (0..1)
    pe_dir_relax: float = 0.10        # the directional threshold is lowered by x here (the pullback trigger is the timing)
    pe_pivot_k: int = 2
    pe_min_m5_bars: int = 40
    pe_disp_atr: float = 1.5          # displacement leg >= x * M5 ATR ...
    pe_disp_body_atr: float = 0.8     # ... with at least one body >= x * M5 ATR
    pe_min_depth: float = 0.30        # never enter shallower than x of the leg (that is chasing)
    pe_max_depth: float = 0.886       # deeper than x = the structure is lost
    pe_zone_chase_atr: float = 0.35   # no entry once price left the pullback zone by > x * M5 ATR
    pe_min_confluence: int = 2        # factors among fibo / OB / S-R / sweep / FVG-zone, at least one not the fibo
    pe_liq_tol_atr: float = 0.12      # equal highs / lows tolerance
    pe_sl_buf_atr: float = 0.12       # structural stop = invalidation level - max(x * M5 ATR, 2 * spread)
    pe_max_sl_atr: float = 1.6        # stop wider than x * M5 ATR -> skip (never widened)
    pe_rr_min: float = 1.5            # reward to the Target LIQ >= x * risk
    pe_tgt_min_atr: float = 0.8       # a target closer than x * M5 ATR is "too near" - the next liquidity is used
    pe_tgt_max_atr: float = 6.0
    pe_near_atr: float = 0.30         # liquidity already within x * M5 ATR in the trade direction = no room
    pe_max_cost_frac: float = 0.30    # spread / stop risk above x = execution cost too high
    pe_range_prem_max: float = 0.85   # pullback path: no BUY in the top x of the M5 range / no SELL in the bottom x (premium / discount)
    # confirmed breakout continuation / retest (the alternative to waiting for a deep pullback)
    pe_breakout_enabled: bool = True
    pe_bo_max_bars: int = 8           # the break must be at most x closed M5 bars old
    pe_bo_body_frac: float = 0.5      # break candle body >= x of its range (not a wick spike)
    pe_bo_beyond_atr: float = 0.1     # ... and closed >= x ATR beyond the broken level
    pe_bo_range_atr: float = 1.0      # ... with range >= x ATR (range expansion)
    pe_bo_cont_ext_atr: float = 0.8   # continuation: price <= x ATR beyond the broken level
    pe_bo_retest_ext_atr: float = 1.5 # after a retest of the level: <= x ATR beyond it
    pe_target_exit: bool = True       # close the basket at the Target LIQ
    pe_invalidation_exit: bool = True # close at once when price takes out the invalidation level (before the wider broker stop)
    max_lot_per_order: float = 50.0
    max_positions_total: int = 128
    max_positions_per_symbol: int = 64   # t40u: batch 16 + 4 recovery layers x 4 + a hedge basket and its layers
    min_orders_per_batch: int = 1     # t35u: 1 (a 2-lot minimum skipped small accounts entirely); never a single order (unless the stop-risk cap cannot fit two min lots)
    # pyramid ADD into a basket that already has its break-even stop: shallow pullback, then price resumes in the
    # trade direction with the signal still valid; same lot (no multiplier), total legs <= max_orders_per_batch
    add_enabled: bool = False         # t45u: no adding just because price pulled back
    add_pullback_atr: float = 0.12    # pullback from the basket's best price >= x * M1 ATR before an add
    add_max_per_step: int = 3
    max_total_lots: float = 400.0
    max_total_notional_x: float = 0.0    # sum of open notional <= x * equity
    max_total_risk_pct: float = 25.0  # RB_MAX_TOTAL_RISK_PCT: sum of open stop risk <= x% of equity
    max_margin_use_pct: float = 90.0  # RB_MAX_MARGIN_USE_PCT: total used margin <= x% of equity
    min_margin_level_pct: float = 110.0
    margin_use_max_pct: float = 100.0 # one order may use <= x% of the free margin
    small_account_max_loss_pct: float = 0.0  # 0 = min lot always allowed (free margin permitting)
    small_account_batch_risk_pct: float = 15.0  # min-lot-floor accounts: ALL open bot stops together <= x% of
                                      # equity (scaled by score 0.6..1.0) -> several min-lot orders per basket
    group_loss_pct: float = 60.0      # RB_GROUP_LOSS_PCT: hard exit when one basket's floating loss >= x% equity
    # NO all-day daily-loss halt. Equity drawdown => SAFE MODE (flatten, pause new entries, resume reduced)
    equity_drawdown_pct: float = 30.0 # RB_MAX_DD_PCT: equity x% below the session peak => safe mode (0 = off)
    safe_pause_s: float = 60.0        # first safe-mode pause; doubles on each repeat in a session ...
    safe_pause_max_s: float = 900.0  # ... up to this
    safe_probe_batches: int = 0       # after a pause: this many single min-lot batches before normal sizing
    safe_min_stopouts: float = 1.5    # min-lot floor accounts: the safe-mode drawdown is never smaller than this
                                      # many full basket stop losses (one normal basket stop must not trip it) ...
    safe_dd_cap_pct: float = 40.0     # ... but never more than x% of the peak equity
    emergency_stale_s: float = 15.0   # open positions and no fresh price for x s => emergency close
    exec_fail_max: int = 3            # entry rejects within 60 s before the symbol is paused
    exec_fail_pause_s: float = 60.0
    close_fail_halt: int = 5          # close attempts failing this many times => halt entries

    # --- broker stop management: hard stop always, then BASKET break-even and dynamic trailing ---
    broker_sl: bool = True            # real broker SL on every order (RB_BROKER_SL=0 to disable)
    be_trigger_atr: float = 0.35      # break-even only after NET basket profit >= max(x * M1 ATR, ...
    be_trigger_spread: float = 3.0    # ... x * spread, stops level + lock) - not so early that noise takes it
    be_lock_spread_mult: float = 0.3  # BE+ level = basket average + costs + x * spread (net profit after costs)
    trail_atr_frac: float = 0.40      # trailing distance = x * M1 ATR (normal momentum) ...
    trail_atr_strong: float = 0.65    # ... wider while momentum is strong (let it run) ...
    trail_atr_fade: float = 0.22      # ... tighter once momentum fades
    trail_spread_mult: float = 2.0    # never closer than x * spread (or the broker stops/freeze level)
    sl_modify_min_interval: float = 1.0
    protect_fail_max: int = 3         # consecutive failed/unverified SL modifies before the basket is closed

    # --- continuous entry / re-entry (no fixed cooldowns; the same unchanged setup is never re-entered) ---
    max_entries_per_minute: int = 40  # API/abuse guard only (TickerAll allows 300 order writes/min per key)
    loss_streak_threshold_add: float = 0.0   # each consecutive losing basket raises the bar by x (max 3x);
                                             # a winning basket resets it. Quality goes up, risk never does.

    # --- learning from real closed groups ---
    learning_enabled: bool = False    # t39u (owner): learner records stats only - NO influence on trading
    learn_min_samples: int = 8
    learn_half_life_hours: float = 0.0   # 0 = never forget (no decay, memory is never reset)
    learn_max_mult: float = 2.0


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
        if cfg.m1_only:
            cfg.precision_enabled = False  # t46u: the M1 fast mode replaces the M5 precision entry
            cfg.no_stop_mode = False       # every order keeps its broker SL
            if "add_enabled" not in data:
                cfg.add_enabled = True     # follow the trend: add on a pullback that resumes (same lot, only into a protected winner)
        if cfg.precision_enabled:
            cfg.no_stop_mode = False       # t45u: the precision entry is defined by its structural stop - never no-SL
        cfg.hedge_enabled = False          # t44u (owner): the hedge / averaging recovery is cancelled - never re-enabled
        cfg.avg_enabled = False            # from a config file, RB_CONFIG_JSON or the runtime tune endpoint
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
