"""Exits/trailing use the broker's executable price when the WS tick is stale or lags (SimBroker +
SYNTHETIC ticks; the REST quote is a test stub)."""
from rapidburst.config import EngineConfig
from tests.harness import make, run
from tests.paths import _small_account, _start
from tests.paths import T0, trend_then_pullback


def _open_position(tr, br):
    t = T0 + 1
    while not tr.own_positions and t < T0 + 200:
        br.advance(t)
        tr.step(t)
        t += 0.3
    assert tr.own_positions
    return t


def test_exec_quote_uses_rest_when_ws_deviates_or_is_stale(tmp_path):
    cfg = EngineConfig()
    cfg.learning_enabled = False
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=100_000.0, margin_per_lot=1.0)
    br.quotes = []
    br.quote = lambda s, max_age=0.4: (br.quotes.append(s) or {"bid": 3000.0, "ask": 3000.2, "recv": 0})
    _start(tr, br, T0 + 1)
    t = _open_position(tr, br)
    p = tr.own_positions[0]
    ps = tr.pos_state[int(p["ticket"])]
    ws = tr.engines["BTCUSDm"].buf.ticks[-1]
    # in sync -> WS (REST is only a periodic check)
    tr._xq.clear(); tr._rest_mode.clear()
    tr._rest_check["BTCUSDm"] = t
    q = tr._exec_quote("BTCUSDm", t + 0.1)
    assert q[2] == "ws"
    # WS stale (> 2 s old): broker REST quote is used
    tr._xq.clear()
    tr.last_receipt["BTCUSDm"] = t - 10
    q = tr._exec_quote("BTCUSDm", t + 0.2)
    assert q[2] == "rest" and q[0] == 3000.0 and br.quotes
    # exit/trailing price follows the broker price (BUY: worst of position price and broker bid)
    px = tr._exec_px(ps, {"price_current": 2650.0}, None, t + 0.2)
    assert px == (3000.2 if ps.side == "SELL" else 2650.0)   # SELL closes at the ask
    # WS lagging far from the broker quote -> REST mode is switched on and logged
    tr._xq.clear(); tr._rest_mode.clear(); tr._rest_check.clear()
    tr.last_receipt["BTCUSDm"] = t + 0.25
    q = tr._exec_quote("BTCUSDm", t + 0.3)
    assert q[2] == "rest" and tr._rest_mode["BTCUSDm"] > t + 0.3
    assert any("FEED LAG" in l["msg"] for l in tr.logs)


def test_modify_results_closed_soft_and_refused_are_handled(tmp_path):
    cfg = EngineConfig()
    cfg.learning_enabled = False
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=100_000.0, margin_per_lot=1.0)
    results = iter([{"ok": False, "closed": True, "msg": "position already closed"},
                    {"ok": False, "soft": True, "msg": "no placeable SL"},
                    {"ok": False, "code": "BROKER_REJECTED", "retcode": 10016, "msg": "HTTP 422 INVALID_STOPS"}])
    br.modify_sl = lambda *a, **k: next(results, {"ok": True, "sl": a[1]})
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 120)
    msgs = [l["msg"] for l in tr.logs]
    assert not any(m.startswith("SL modify") and "closed" in m for m in msgs)       # closed -> not a refusal
    assert not any("no placeable" in m for m in msgs)                              # soft -> not logged as refusal
