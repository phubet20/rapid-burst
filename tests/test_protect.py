"""Basket break-even + dynamic trailing (verified modifies), RSI soft feature, intrabar re-entry stats.
SYNTHETIC ticks/bars on the simulated broker (test double) - not market results."""
import math

from rapidburst.config import EngineConfig
from rapidburst.mtf import TF
from tests.harness import make, run
from tests.paths import T0, _small_account, _start, series_up


def _wilder_rsi(closes, n=14):
    ag = al = 0.0
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        k = min(i, n)
        ag += (max(ch, 0.0) - ag) / k
        al += (max(-ch, 0.0) - al) / k
    return 100.0 - 100.0 / (1.0 + ag / al) if al else 100.0


def test_incremental_rsi_matches_brute_force_and_is_bounded():
    closes = [100 + math.sin(i / 5) * 2 + (i % 3) * 0.1 for i in range(80)]
    tf = TF(60)
    bars = [{"time": 1_800_000_000 + i * 60, "open": c, "high": c + 0.2, "low": c - 0.2, "close": c, "tick_volume": 50}
            for i, c in enumerate(closes)]
    tf.load(bars + [dict(bars[-1], time=bars[-1]["time"] + 60)], bars[-1]["time"] + 70)
    assert abs(tf.st.rsi - _wilder_rsi(closes)) < 1e-9
    st = tf.state(bars[-1]["time"] + 70)
    assert st.rsi_ready and 0.0 <= st.rsi <= 100.0


def _session(tmp_path, fail=False):
    tr, br = make(tmp_path, {"BTCUSDm": series_up()})
    _small_account(br, balance=1000.0, margin_per_lot=100.0)
    if fail:
        br.modify_sl = lambda t, sl, tp: {"ok": False, "msg": "TEST rejected"}
    hist = {}
    _start(tr, br, T0 + 1)

    def on(t):
        for p in br.positions_.values():
            hist.setdefault(p["ticket"], []).append(p["sl"])
    run(tr, br, T0 + 1, series_up()[-1][0], on_step=on)
    return tr, br, hist


def test_basket_break_even_then_dynamic_trailing_only_forward(tmp_path):
    tr, br, hist = _session(tmp_path)
    msgs = [l["msg"] for l in tr.logs]
    be = [m for m in msgs if m.startswith("BREAK-EVEN")]
    assert be, msgs[-30:]
    ps = tr.prot_stats
    assert ps["be"] >= 1 and ps["trail"] >= 1 and ps["modify_ok"] >= 1 and ps["modify_fail"] == 0
    for t, sls in hist.items():                     # BUY stops only ever move forward, and never disappear
        live = [x for x in sls if x]
        assert live == sorted(live) and all(sls), (t, sls)
    trail_msgs = [m for m in msgs if m.startswith("SL #") and "trail" in m]
    assert any("momentum strong" in m for m in trail_msgs) or any("momentum normal" in m for m in trail_msgs)
    perf = tr._perf_view()
    assert perf["protection"]["baskets_with_be"] >= 1 and "entries_per_m1_bar" in perf
    assert perf["source"] == "real broker deals"     # here: the simulated broker's deals (test double)


def test_failed_sl_modify_is_never_assumed_protected_and_closes_the_basket(tmp_path):
    tr, br, _ = _session(tmp_path, fail=True)
    msgs = [l["msg"] for l in tr.logs]
    warn = [m for m in msgs if "NOT confirmed" in m]
    assert warn and all("unprotected at the broker" in m for m in warn)
    assert any(m.startswith("EXIT GROUP") and "protection failed" in m for m in msgs)
    assert tr.prot_stats["protect_close"] >= 1 and tr.prot_stats["modify_ok"] == 0


def test_intrabar_reentry_cycles_are_counted_not_limited(tmp_path):
    tr, br, _ = _session(tmp_path)
    perf = tr._perf_view()
    per_bar = perf["entries_per_m1_bar"]
    assert per_bar["bars"] >= 1 and per_bar["max"] >= 2     # several baskets in one M1 bar - no candle lock
    assert perf["reentry_s"]["n"] >= 1
    st = tr._status("BTCUSDm", *tr._micro("BTCUSDm", br.now), br.now)
    assert set(st["m1_cycles"]) == {"bar", "opened", "closed"}


def test_batch_cap_is_8_but_never_forced(tmp_path):
    cfg = EngineConfig()
    assert cfg.max_orders_per_batch == 8 and cfg.max_positions_per_symbol >= 8
    tr, br, _ = _session(tmp_path)
    sizes = [int(m.split(":")[1].split("/")[0]) for m in (l["msg"] for l in tr.logs) if m.startswith("BATCH ")]
    assert sizes and max(sizes) <= 8 and min(sizes) < 8
