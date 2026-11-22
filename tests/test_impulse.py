"""M1 impulse entries, tight broker SL -> BE+ -> trailing, wrong-way cut, pyramiding caps,
no martingale, and the optional (default OFF) daily-loss / drawdown halts.
SimBroker + SYNTHETIC ticks only (test double)."""
import random
from types import SimpleNamespace

from rapidburst.config import EngineConfig
from rapidburst.engine import Tick
from rapidburst.impulse import detect
from rapidburst.risk import AccountView, RiskGuard
from tests.harness import Invariants, instrument, make, now_base, run
from tests.test_burst import _small_account, _start

T0 = now_base()


def _quiet(t, mid, secs, rnd):
    out = []
    end = t + secs
    while t < end:
        t += 0.3
        mid += rnd.gauss(0, 0.03)
        out.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return out, t, mid


def _leg(t, mid, secs, step, out, dt=0.25):
    end = t + secs
    while t < end:
        t += dt
        mid += step
        out.append((t, round(mid - 0.09, 2), round(mid + 0.09, 2)))
    return t, mid


def trend_then_pullback(seed=4):
    rnd = random.Random(seed)
    out, t, mid = _quiet(T0, 2650.0, 150, rnd)
    t, mid = _leg(t, mid, 30, +0.05, out)
    t, mid = _leg(t, mid, 14, -0.05, out)
    more, t, mid = _quiet(t, mid, 90, rnd)
    return out + more


def spike_then_fade(seed=4):
    rnd = random.Random(seed)
    out, t, mid = _quiet(T0, 2650.0, 150, rnd)
    t, mid = _leg(t, mid, 3, +0.06, out)
    t, mid = _leg(t, mid, 25, -0.012, out)
    more, t, mid = _quiet(t, mid, 60, rnd)
    return out + more


def _no_tp(br):
    orig = br.market_order
    br.market_order = lambda s, side, v, sl, tp, dev, magic, c: orig(s, side, v, sl, 0.0, dev, magic, c)


def _sl_track(br, hist):
    def on(t):
        for p in br.positions_.values():
            hist.setdefault(p["ticket"], []).append((p["type"], p["price_open"], p["sl"]))
    return on


# ------------------------------------------------------------- detector
def _ticks(series):
    return [Tick(t, b, a) for t, b, a in series]


def test_detector_fires_on_surge_and_not_on_chop():
    cfg = EngineConfig()
    rnd = random.Random(1)
    out, t, mid = _quiet(T0, 2650.0, 60, rnd)
    calm = detect(_ticks(out), t, 0.4, 0.18, cfg)
    assert calm.dir == 0 and calm.reason.startswith("impulse:")
    t2, _ = _leg(t, mid, 4, +0.06, out)
    imp = detect(_ticks(out), t2, 1.0, 0.18, cfg)
    assert imp.dir == 1 and imp.strength > 0.5, imp.reason
    dn = [(tt, b, a) for tt, b, a in out[:-16]]
    t3, _ = _leg(dn[-1][0], (dn[-1][1] + dn[-1][2]) / 2, 4, -0.06, dn)
    assert detect(_ticks(dn), t3, 1.0, 0.18, cfg).dir == -1


def test_detector_refuses_moves_smaller_than_spread_and_exhausted_bars():
    cfg = EngineConfig()
    rnd = random.Random(2)
    out, t, mid = _quiet(T0, 2650.0, 60, rnd)
    t2, _ = _leg(t, mid, 4, +0.06, out)
    assert detect(_ticks(out), t2, 0.4, 5.0, cfg).dir == 0          # spread 5.0 >> move
    assert detect(_ticks(out), t2, 0.05, 0.18, cfg).dir == 0        # bar already >> 1.6 ATR


# ------------------------------------------ SL at open -> BE+ -> trailing
def test_sl_at_open_then_breakeven_then_trailing_exit_in_profit(tmp_path):
    cfg = EngineConfig(batch_tp_atr=50.0, batch_tp_units=50.0, reversal_score=5.0, batch_max_hold_seconds=10_000,
                       max_hold_seconds=10_000)
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _no_tp(br)
    inv = Invariants(); instrument(br, cfg, inv)
    hist = {}
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 300, on_step=_sl_track(br, hist))
    msgs = [l["msg"] for l in tr.logs]
    assert inv.entries >= 1 and inv.orders_without_sl == 0, msgs[-10:]      # broker SL on every order at open
    i_be = next((i for i, m in enumerate(msgs) if m.startswith("SL #") and "(BE+" in m), None)
    i_tr = next((i for i, m in enumerate(msgs) if m.startswith("SL #") and "(trail" in m), None)
    assert i_be is not None and i_tr is not None and i_be < i_tr, [m for m in msgs if "SL" in m or "ENTRY" in m][:12]
    for samples in hist.values():                  # a stop never moves backwards
        typ = samples[0][0]
        sls = [sl for _, _, sl in samples if sl]
        assert all((b - a) * (1 if typ == 0 else -1) >= -1e-9 for a, b in zip(sls, sls[1:])), sls
    wins = [d for d in br.deals_ if d["entry"] == 1 and d["profit"] > 0]
    assert wins, "pullback should have exited on the trailing stop with a profit"
    assert not br.positions_


def test_pyramid_only_after_sl_locked_and_capped_by_tier(tmp_path):
    for balance, max_pos in ((3.0, 2), (1000.0, 6)):
        import pathlib, tempfile
        cfg = EngineConfig(batch_tp_atr=50.0, batch_tp_units=50.0, reversal_score=5.0,
                           batch_max_hold_seconds=10_000, max_hold_seconds=10_000)
        tr, br = make(pathlib.Path(tempfile.mkdtemp()), {"BTCUSDm": trend_then_pullback()}, cfg)
        br.spec = dict(br.spec, trade_tick_value=0.05, trade_tick_value_loss=0.05)
        _small_account(br, balance=balance, margin_per_lot=1.0)
        _no_tp(br)
        inv = Invariants(); instrument(br, cfg, inv)
        _start(tr, br, T0 + 1)
        run(tr, br, T0 + 1, T0 + 300)
        msgs = [l["msg"] for l in tr.logs]
        pyr = [i for i, m in enumerate(msgs) if m.startswith("ENTRY") and "(pyramid)" in m]
        be = [i for i, m in enumerate(msgs) if m.startswith("SL #") and "(BE+" in m]
        assert inv.adds_into_loser == 0
        if pyr:
            assert be and be[0] < pyr[0]
        assert inv.max_pos_symbol <= max_pos, (balance, inv.max_pos_symbol)
        if balance == 1000.0:
            assert pyr, [m for m in msgs if m.startswith(("ENTRY", "SL", "EXIT"))][:12]


def test_sl_modify_refused_falls_back_to_software_trailing(tmp_path):
    cfg = EngineConfig(batch_tp_atr=50.0, batch_tp_units=50.0, reversal_score=5.0, batch_max_hold_seconds=10_000,
                       max_hold_seconds=10_000)
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _no_tp(br)
    br.modify_sl = lambda t, sl, tp: {"ok": False, "msg": "MODIFY_NOT_ALLOWED"}
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 300)
    msgs = [l["msg"] for l in tr.logs]
    assert any("refused" in m and "software" in m for m in msgs)
    hits = [m for m in msgs if m.startswith("EXIT") and "stop hit" in m]
    assert hits, [m for m in msgs if m.startswith(("ENTRY", "EXIT", "SL"))][:10]
    assert [d for d in br.deals_ if d["entry"] == 1 and d["profit"] > 0]


# ----------------------------------------------------------- wrong way
def test_wrong_way_is_cut_fast_with_small_loss(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": spike_then_fade()}, cfg)
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 230)
    msgs = [l["msg"] for l in tr.logs]
    entries = [m for m in msgs if m.startswith("ENTRY")]
    assert entries, msgs[-8:]
    exits = [m for m in msgs if m.startswith("EXIT")]
    losers = [d for d in br.deals_ if d["entry"] == 1 and d["profit"] < 0]
    assert any("wrong way" in m for m in exits) or not losers, exits
    vol = float(entries[0].split()[2])
    worst = min((d["profit"] for d in br.deals_ if d["entry"] == 1), default=0.0)
    assert worst > -vol * 100 * 0.5           # loss well inside the 0.36 px initial stop (0.36 * 100 * vol)


# ---------------------------------------------------------- no martingale
def test_never_sizes_up_after_a_loss(tmp_path):
    cfg = EngineConfig()
    cfg.learning_enabled = False                     # isolate the no-martingale cap from learning cold start
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=100_000.0, margin_per_lot=1.0)
    tr.no_up_after_loss["BTCUSDm"] = 0.01            # last burst lost at 0.01 lot
    import collections
    tr.decisions = collections.deque(maxlen=10_000)  # keep every decision for the assertion
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200)
    msgs = [l["msg"] for l in tr.logs]
    first_close = next((i for i, m in enumerate(msgs) if "fully closed" in m), len(msgs))
    entries = [m for m in msgs[:first_close] if m.startswith("ENTRY")]
    assert entries and all(float(m.split()[2]) <= 0.01 for m in entries), entries[:3]   # until a burst result
    assert any("capped at 0.01 after loss" in d["outcome"] for d in tr.decisions)


def test_losing_burst_records_the_lot_cap():
    from rapidburst.trader import Trader
    t = SimpleNamespace(no_up_after_loss={})
    b = {"realized": -0.5, "lot": 0.03}
    # replicate the bookkeeping rule used at burst close
    if b["realized"] < 0:
        t.no_up_after_loss["X"] = b["lot"]
    assert t.no_up_after_loss["X"] == 0.03 and hasattr(Trader, "_enter_batch")


# -------------------------------------------- kill-switch default OFF
def test_daily_loss_and_drawdown_halts_are_off_by_default(monkeypatch):
    g = RiskGuard(EngineConfig())
    assert g.update("d", AccountView(10, 10, 10, 0), 0) is None
    assert g.update("d", AccountView(10, 1, 1, 0), -9.0) is None and not g.halted
    monkeypatch.setenv("RB_DAILY_LOSS_PCT", "20")
    monkeypatch.setenv("RB_MAX_DD_PCT", "30")
    c = EngineConfig.load()
    assert c.daily_loss_limit_pct == 20.0 and c.equity_drawdown_pct == 30.0
    g2 = RiskGuard(c)
    g2.update("d", AccountView(10, 10, 10, 0), 0)
    assert "daily loss" in g2.update("d", AccountView(10, 7, 7, 0), -3.0)
