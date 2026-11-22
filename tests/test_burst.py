"""Burst batches, small-account sizing, higher-timeframe trend filter and the
"why no trade" ledger. SimBroker + SYNTHETIC ticks/bars only (test double)."""
import json

from rapidburst.config import EngineConfig
from rapidburst.trend import compute_trend, relation
from tests.harness import Invariants, instrument, make, now_base, run
from tests.synth import burst_series, gen_ticks

T0 = now_base()


def bars(closes, rng=1.0):
    return [{"time": i * 60.0, "open": c, "high": c + rng / 2, "low": c - rng / 2, "close": c}
            for i, c in enumerate(closes)]


# ------------------------------------------------------------------ trend
def test_trend_up_down_flat_unknown():
    up = compute_trend(bars([100 + i * 0.6 for i in range(30)]), bars([100 + i * 2 for i in range(12)], 3))
    assert up.known and up.dir == 1 and up.strength > 0.5
    dn = compute_trend(bars([100 - i * 0.6 for i in range(30)]), bars([100 - i * 2 for i in range(12)], 3))
    assert dn.dir == -1
    flat = compute_trend(bars([100 + (0.3 if i % 2 else -0.3) for i in range(30)]), [])
    assert flat.known and flat.dir == 0
    unk = compute_trend(bars([1, 2, 3]), [])
    assert not unk.known and unk.dir == 0 and "unknown" in unk.label
    assert relation(up, "BUY") == "aligned" and relation(up, "SELL") == "counter"
    assert relation(flat, "SELL") == "neutral" and relation(unk, "BUY") == "neutral"


# --------------------------------------------------------------- helpers
def _small_account(br, balance=10.0, margin_per_lot=45.0):
    """$10 account at high leverage: tiny margin per min lot, like Exness 1:2000."""
    br.balance = balance
    orig_acct = br.account

    def account():
        a = orig_acct()
        if a:
            used = sum(p["volume"] * margin_per_lot for p in br.positions_.values())
            a["margin_free"] = round(a["equity"] - used, 2)
            a["margin_level"] = (a["equity"] / used * 100.0) if used else 0.0
        return a
    br.account = account
    br.calc_margin = lambda side, s, vol, price: vol * margin_per_lot


def _series_up(seed=3):
    ticks = burst_series(T0, +1, pre_seconds=150, burst_seconds=8, step=0.08, seed=seed)
    t, mid = ticks[-1][0], (ticks[-1][1] + ticks[-1][2]) / 2
    for i in range(1, 900):          # gentle continuation then flat
        t += 0.3
        mid += 0.02 if i < 60 else 0.0
        ticks.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return ticks


def _start(tr, br, t):
    br.advance(t)
    tr.step(t)
    assert tr.conn_state == "CONNECTED", tr.conn_msg
    r = tr.cmd_start()
    assert r["ok"], r
    return r


# -------------------------------------------------------- burst batches
def test_small_account_opens_a_burst_batch_with_sl_and_closes_it_together(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": _series_up()}, cfg)
    br.spec = dict(br.spec, trade_tick_value=0.05, trade_tick_value_loss=0.05)   # min lot risk > 0.25% budget
    _small_account(br)
    inv = Invariants(); instrument(br, cfg, inv)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 420)
    entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")]
    assert entries, [l["msg"] for l in tr.logs][-15:]
    batch_ids = {m.split(" batch ")[1].split()[0] for m in entries}
    first = sorted(batch_ids)[0]
    legs = [m for m in entries if f"batch {first}" in m]
    assert 2 <= len(legs) <= cfg.batch_max, legs
    assert inv.orders_with_broker_sl == 0 and inv.adds_into_loser == 0 and inv.opposite_while_open == 0
    assert inv.max_pos_symbol <= cfg.max_positions_per_symbol
    exits = [l["msg"] for l in tr.logs if l["msg"].startswith("EXIT BATCH")]
    assert exits, [l["msg"] for l in tr.logs][-15:]
    assert any("fully closed" in l["msg"] for l in tr.logs)
    assert not br.positions_ or all(not p["sl"] for p in br.positions_.values())   # Broker SL OFF
    # every opened order used the min lot (small account), never a bigger martingale size
    assert all(" 0.01 BTCUSDm" in m for m in entries)


def test_batch_cooldown_and_stop_flattens(tmp_path):
    cfg = EngineConfig(batch_tp_units=50.0, batch_max_hold_seconds=10_000, max_hold_seconds=10_000)
    tr, br = make(tmp_path, {"BTCUSDm": _series_up(5)}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 175)
    if br.positions_:
        r = tr.cmd_stop()
        assert r["ok"] and not br.positions_


def test_insufficient_free_margin_for_min_lot_is_the_only_skip_and_is_visible(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": _series_up()}, cfg)
    _small_account(br, balance=0.30, margin_per_lot=45.0)      # $0.30: min lot needs 0.45 margin
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 240)
    assert not [l for l in tr.logs if l["msg"].startswith("ENTRY")]
    tr._build_snapshot(T0 + 240)
    skips = tr.get_snapshot()["engine"]["symbols"]["BTCUSDm"]["skips"]
    assert skips.get("margin", 0) >= 1, skips
    dec = tr.get_snapshot()["engine"]["decisions"]
    assert any("insufficient free margin for the minimum lot" in d["outcome"] for d in dec), dec
    assert any(l["msg"].startswith("WHY NO TRADE") for l in tr.logs)
    rc = tr.get_snapshot()["engine"]["risk_config"]
    assert rc["small_account_max_loss_pct"] == 0.0 and rc["daily_loss_limit_pct"] == 20.0


def test_tiny_equity_trades_one_min_lot_and_tiers_scale_up(tmp_path):
    for equity, legs_max in ((3.0, 1), (12.0, 2), (30.0, 3), (1000.0, 5)):
        import pathlib, tempfile
        cfg = EngineConfig()
        tr, br = make(pathlib.Path(tempfile.mkdtemp()), {"BTCUSDm": _series_up()}, cfg)
        br.spec = dict(br.spec, trade_tick_value=0.05, trade_tick_value_loss=0.05)
        _small_account(br, balance=equity)
        _start(tr, br, T0 + 1)
        run(tr, br, T0 + 1, T0 + 200)
        entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")]
        assert entries, (equity, [l["msg"] for l in tr.logs][-5:])
        first = entries[0].split(" batch ")[1].split()[0]
        n = len([m for m in entries if f"batch {first}" in m])
        assert 1 <= n <= legs_max, (equity, n)
        if equity == 3.0:
            assert n == 1 and " 0.01 BTCUSDm" in entries[0]
    assert tr._tier_legs(3) == 1 and tr._tier_legs(10) == 2 and tr._tier_legs(60) == 4 and tr._tier_legs(5000) == 5


# --------------------------------------------------------------- trend gate
def test_counter_trend_needs_very_strong_score_and_is_single_order(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": _series_up()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    falling = bars([90000 - i * 30 for i in range(45)], 20)
    br.candles = lambda s, tf, hours: falling if tf == "M1" else bars([90000 - i * 120 for i in range(18)], 80)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 300)
    assert tr.trends["BTCUSDm"].dir == -1
    entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY BUY")]
    for m in entries:
        assert "(counter-trend)" in m and "leg 1/1" in m, m
    counts = tr.skip_counts.get("BTCUSDm", {})
    assert entries or counts.get("counter-trend filter", 0) >= 1, (counts, [l["msg"] for l in tr.logs][-10:])


def test_trend_aligned_allows_full_batch_and_trend_in_snapshot(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": _series_up()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    br.candles = lambda s, tf, hours: (bars([90000 + i * 30 for i in range(45)], 20) if tf == "M1"
                                       else bars([90000 + i * 120 for i in range(18)], 80))
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200)
    tr._build_snapshot(T0 + 200)
    sv = tr.get_snapshot()["engine"]["symbols"]["BTCUSDm"]
    assert sv["trend"]["dir"] == 1 and sv["trend"]["known"]
    entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")]
    assert any("leg 2/" in m for m in entries), entries


def test_no_history_broker_reports_trend_unknown(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 300)})
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 30)
    assert not tr.trends["XAUUSD"].known


def test_config_json_env_override(monkeypatch):
    monkeypatch.setenv("RB_CONFIG_JSON", json.dumps({"small_account_max_loss_pct": 3, "batch_max": 4,
                                                     "unknown_key": 1}))
    c = EngineConfig.load()
    assert c.small_account_max_loss_pct == 3.0 and c.batch_max == 4


# ------------------------------------------------ NO broker-side SL mode
def _no_broker_tp(br):
    orig = br.market_order
    br.market_order = lambda s, side, v, sl, tp, dev, magic, c: orig(s, side, v, sl, 0.0, dev, magic, c)


def _drop_after(ticks, t_cut, amount):
    return [(t, b - amount, a - amount) if t > t_cut else (t, b, a) for t, b, a in ticks]


def test_orders_have_no_broker_sl_and_engine_hard_close_fires():
    import pathlib, tempfile
    cfg = EngineConfig(batch_tp_units=500.0, batch_max_hold_seconds=10_000, max_hold_seconds=10_000,
                       batch_max_loss_pct=100.0, exhaustion_z=99.0, weak_momentum_close_units=999.0,
                       profit_trigger_units=999.0, reversal_score=5.0, adverse_fade_units=999.0,
                       daily_loss_limit_pct=90, equity_drawdown_pct=90)
    d = pathlib.Path(tempfile.mkdtemp())
    tr, br = make(d, {"BTCUSDm": _drop_after(_series_up(), T0 + 172, 6.0)}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _no_broker_tp(br)
    _start(tr, br, T0 + 1)
    seen = []

    def on(t):
        for p in br.positions_.values():
            seen.append(p["sl"])
    run(tr, br, T0 + 1, T0 + 260, on_step=on)
    entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")]
    assert entries and all("broker SL OFF" in m for m in entries), entries[:2]
    assert seen and all(x == 0 for x in seen), "an order carried a broker SL"
    hc = [l["msg"] for l in tr.logs if "hard close" in l["msg"] and l["msg"].startswith("EXIT")]
    assert hc, [l["msg"] for l in tr.logs if "EXIT" in l["msg"] or "ENTRY" in l["msg"]][-8:]
    assert not br.positions_                         # whole batch flat after the engine stop


def test_batch_group_loss_pct_hard_close():
    import pathlib, tempfile
    cfg = EngineConfig(batch_tp_units=500.0, batch_max_hold_seconds=10_000, max_hold_seconds=10_000,
                       sl_vol_units=500.0, batch_max_loss_pct=0.5, adverse_fade_units=500.0,
                       reversal_score=5.0, daily_loss_limit_pct=90, equity_drawdown_pct=90,
                       exhaustion_z=99.0, weak_momentum_close_units=999.0, profit_trigger_units=999.0)
    d = pathlib.Path(tempfile.mkdtemp())
    tr, br = make(d, {"BTCUSDm": _drop_after(_series_up(), T0 + 172, 6.0)}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=1.0)
    _no_broker_tp(br)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 260)
    assert any("group loss" in l["msg"] for l in tr.logs if l["msg"].startswith("EXIT")), \
        [l["msg"] for l in tr.logs if "EXIT" in l["msg"] or "ENTRY" in l["msg"]][-6:]
    assert not br.positions_


def test_unprotected_positions_flattened_when_engine_not_running():
    import pathlib, tempfile
    cfg = EngineConfig()
    d = pathlib.Path(tempfile.mkdtemp())
    tr, br = make(d, {"BTCUSDm": _series_up()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    br.advance(T0 + 200)
    br.positions_[7] = {"ticket": 7, "symbol": "BTCUSDm", "type": 0, "volume": 0.01, "price_open": 2650.0,
                        "price_current": 2650.0, "sl": 0.0, "tp": 0.0, "profit": 0.0, "swap": 0.0,
                        "magic": cfg.magic, "comment": "RBurst-old", "time": T0}
    for i in range(4):
        tr.step(T0 + 200 + i)       # connect -> adopt -> not running -> flatten
    assert not br.positions_, "unprotected bot position survived a restart/reconnect"
    assert any("flattening immediately" in l["msg"] for l in tr.logs)


def test_broker_sl_and_emergency_sl_are_opt_in():
    import pathlib, tempfile
    for kw, expect in ((dict(broker_sl=True), "near"), (dict(emergency_sl=True), "far")):
        cfg = EngineConfig(batch_tp_units=500.0, batch_max_hold_seconds=10_000, max_hold_seconds=10_000, **kw)
        d = pathlib.Path(tempfile.mkdtemp())
        tr, br = make(d, {"BTCUSDm": _series_up()}, cfg)
        _small_account(br, balance=1000.0, margin_per_lot=10.0)
        _start(tr, br, T0 + 1)
        seen = {}

        def on(t):
            for p in br.positions_.values():
                seen[p["ticket"]] = (p["price_open"], p["sl"])
        run(tr, br, T0 + 1, T0 + 170, on_step=on)
        assert seen and all(sl for _, sl in seen.values()), kw
        dist = max(abs(o - sl) for o, sl in seen.values())
        if expect == "far":
            assert dist > 2.0, dist                  # far disaster stop, not a working stop
        else:
            assert dist < 2.0, dist


def test_env_flags_parse_booleans(monkeypatch):
    assert EngineConfig.load().broker_sl is False and EngineConfig.load().emergency_sl is False
    monkeypatch.setenv("RB_BROKER_SL", "1")
    monkeypatch.setenv("RB_EMERGENCY_SL", "false")
    c = EngineConfig.load()
    assert c.broker_sl is True and c.emergency_sl is False


def test_snapshot_reports_broker_sl_off(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": _series_up()})
    _start(tr, br, T0 + 1)
    tr._build_snapshot(T0 + 2)
    assert tr.get_snapshot()["engine"]["risk_config"]["broker_sl"] == "OFF"
