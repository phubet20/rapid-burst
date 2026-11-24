"""t34u: M5 momentum auto-reversal + continuous re-entry state machine (SimBroker test double, not market data)."""
from rapidburst.config import EngineConfig
from rapidburst.microburst import decide
from rapidburst.mtf import MTF
from tests.harness import legacy, make, run
from tests.paths import T0, _small_account, _start, series_up
from tests.test_mtf import _live, _view


def _controlled(tr):
    """_micro with a test-controlled signal: ctl["side"] in BUY / SELL / NO TRADE."""
    real = tr._micro
    ctl = {"side": "NO TRADE"}

    def fake(s, now_wall):
        m, st = real(s, now_wall)
        if m.ready and ctl["side"] != "NO TRADE":
            st.side, st.origin, st.reason = ctl["side"], 1.0, f"{ctl['side']} (test signal)"
            st.buy, st.sell = (0.9, 0.2) if ctl["side"] == "BUY" else (0.2, 0.9)
        elif m.ready:
            st.side, st.buy, st.sell = "NO TRADE", 0.3, 0.3
        return m, st
    tr._micro = fake
    return ctl


def _session(tmp_path):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()}, legacy(add_enabled=False))
    _small_account(br, balance=1000.0, margin_per_lot=10.0)
    _start(tr, br, T0 + 1)
    ctl = _controlled(tr)
    tr._micro_exit = lambda *a, **k: None          # isolate the reversal path from the other exits
    sent = []
    orig = br.market_order

    def mo(*a, **k):
        r = orig(*a, **k)
        sent.append((br.now, a[1], r.get("ok")))
        return r
    br.market_order = mo
    return tr, br, ctl, sent


def test_buy_to_sell_reversal_closes_verifies_and_opens_in_the_same_loop(tmp_path):
    tr, br, ctl, sent = _session(tmp_path)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    buys = [p for p in br.positions_.values() if p["type"] == 0]
    assert len(buys) >= 2 and all(p["sl"] for p in buys)
    ctl["side"] = "SELL"
    t0 = t = T0 + 3.3

    def until_flip(t, typ):
        # t36u: the reversal must persist ~0.8 s on clean ticks (anti-jitter) before the close; then close
        # (verified) and open the opposite batch in that SAME loop
        while t < t0 + 10 and not (br.positions_ and all(p["type"] == typ for p in br.positions_.values())):
            br.advance(t)
            tr.step(t)
            t += 0.2
        return t - 0.2
    t = until_flip(t, 1)
    assert 0.7 <= t - t0 <= 1.6, t - t0
    assert all(p["type"] == 1 for p in br.positions_.values()) and len(br.positions_) >= 2
    sells = [x for x in sent if x[1] == "SELL"]
    assert sells and sells[0][0] == t                     # opened in the same loop as the verified close
    msgs = [l["msg"] for l in tr.logs]
    assert any(m.startswith("EXIT GROUP") and "REVERSAL: SELL" in m and "FAIL" not in m for m in msgs)
    assert tr.reversals == 1
    # SELL -> BUY the same way
    ctl["side"] = "BUY"
    t0 = t = t + 0.2
    t = until_flip(t, 0)
    assert all(p["type"] == 0 for p in br.positions_.values()) and br.positions_ and tr.reversals == 2


def test_failed_close_never_opens_the_opposite_side(tmp_path):
    tr, br, ctl, sent = _session(tmp_path)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    n_buy = len(br.positions_)
    br.close_position = lambda *a, **k: {"ok": False, "verified": False, "msg": "TEST requote"}
    ctl["side"] = "SELL"
    run(tr, br, T0 + 3.3, T0 + 5)
    assert len(br.positions_) == n_buy and all(p["type"] == 0 for p in br.positions_.values())   # no hedge
    assert not [x for x in sent if x[1] == "SELL"]
    assert any("FAIL TEST requote" in l["msg"] for l in tr.logs)


def test_no_lock_after_close_reentry_within_one_loop_and_many_per_m5_bar(tmp_path):
    tr, br, ctl, sent = _session(tmp_path)
    ctl["side"] = "BUY"
    entries = []
    run(tr, br, T0 + 1.3, T0 + 3)                         # warm-up + first entry
    t = T0 + 3.3
    for tk in list(br.positions_):
        br._close(tk, br._tick("BTCUSDm")[1], tr.cfg.magic)
    for k in range(4):                                    # open, close (flatten), re-enter at once - 4 cycles
        run(tr, br, t, t + 0.6)
        assert br.positions_, f"cycle {k}: no entry"
        entries.append(min(p["time"] for p in br.positions_.values()))
        for tk in list(br.positions_):
            br._close(tk, br._tick("BTCUSDm")[1], tr.cfg.magic)      # basket closed at the broker
        t += 0.9
    assert len(entries) == 4 and entries[-1] - entries[0] < 300          # 4 batches inside one M5 bar
    reent = [r for r in tr.reentry]
    assert reent and max(reent) <= 1.0, reent                             # re-entry within a loop or two
    assert not any("duplicate" in l["msg"] or "cooldown" in l["msg"].lower() for l in tr.logs)


def test_absence_of_buy_is_not_sell_and_m5_momentum_is_family_e():
    cfg = legacy()
    flat = decide(_live(vel=0.0, accel=0.0, imbalance=0.0, bidask=0.0, body=0.0, breakout=0, burst_z=0.5,
                        seq_run=0, seq_consist=0.0, eff=0.0, mtf=_view(0.0)), cfg)
    assert flat.side == "NO TRADE"
    v = _view(0.6)
    v.m5.ready, v.m5_up, v.m5_dn, v.m5_bias = True, 0.8, 0.05, 0.5
    st = decide(_live(mtf=v), cfg)
    assert st.side == "BUY" and abs(st.scan["BUY"]["E"] - 0.8) < 1e-9


def test_mtf_m5_forming_bar_without_close_wait():
    mt = MTF()
    t, px = 1_700_000_000.0, 100.0
    for i in range(7 * 300):                               # 35 min of rising ticks -> M5 bars from ticks
        px += 0.002
        mt.on_tick(t + i, px)
    v = mt.view(t + 7 * 300 + 30)                         # 30 s into a forming M5 bar
    assert v.m5.ready and v.m5_up > v.m5_dn and v.m5_bias > 0


def test_t35u_entry_defaults_are_minimal_but_risk_guards_stay():
    c = EngineConfig(precision_enabled=False, add_enabled=True)
    assert c.mb_threshold == 0.40 and c.mb_edge == 0.06 and c.min_orders_per_batch == 1     # t44u: edge 0.03 -> 0.06
    assert c.warmup_seconds <= 1.0 and c.reverse_enabled
    assert c.small_account_batch_risk_pct > 0 and c.max_orders_per_batch == 16 and not c.no_stop_mode
