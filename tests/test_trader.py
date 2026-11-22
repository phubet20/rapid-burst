"""Integration tests of the real Trader loop with the SimBroker TEST DOUBLE on SYNTHETIC ticks."""
import base64
import json

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from rapidburst.config import EngineConfig
from tests.harness import Invariants, instrument, make, now_base, run
from tests.synth import burst_series, gen_ticks

T0 = now_base()


def connect_and_start(trader, broker, t):
    broker.advance(t)
    trader.step(t)
    assert trader.conn_state == "CONNECTED", trader.conn_msg
    return trader.cmd_start()


def test_live_account_refused(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(1, T0, 900)}, trade_mode=2)
    r = connect_and_start(tr, br, T0 + 1)
    assert not r["ok"] and "Account type" in r["msg"]
    run(tr, br, T0 + 1, T0 + 900)
    assert br.sent_orders == 0
    assert any(c["name"] == "Account type" and "LIVE trading is disabled" in c["detail"] for c in tr.validation["checks"])


def test_live_needs_server_flag(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(1, T0, 300)}, trade_mode=2)
    tr.allow_live = True
    assert connect_and_start(tr, br, T0 + 1)["ok"]


def test_algo_trading_disabled_blocks_start(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(1, T0, 300)}, algo=False)
    r = connect_and_start(tr, br, T0 + 1)
    assert not r["ok"] and "Algo trading" in r["msg"]


def test_no_credentials_no_start(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(1, T0, 300)})
    tr.store.forget_credentials()
    br.advance(T0 + 1); tr.step(T0 + 1)
    assert tr.conn_state == "NO_CREDENTIALS"
    assert not tr.cmd_start()["ok"]


def test_autonomous_session_respects_invariants(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 3600)}, cfg)
    inv = Invariants(); instrument(br, cfg, inv)
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    run(tr, br, T0 + 1, T0 + 3600)
    assert inv.entries >= 5, inv.entries
    assert inv.orders_without_sl == 0
    assert inv.adds_into_loser == 0
    assert inv.opposite_while_open == 0
    assert inv.max_pos <= cfg.max_positions_total
    assert inv.max_pos_symbol <= cfg.max_positions_per_symbol
    assert inv.max_lots <= cfg.max_total_lots + 1e-9
    closes = [l for l in tr.logs if l["msg"].startswith("EXIT")]
    assert closes, "engine never exited a trade by itself"


def test_stop_closes_only_bot_positions(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 3600)}, cfg)
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    manual = br.add_foreign_position("XAUUSD", "SELL", 0.05, magic=0)
    other_ea = br.add_foreign_position("XAUUSD", "BUY", 0.02, magic=777)
    own_pending = br.add_pending("XAUUSD", cfg.magic)
    foreign_pending = br.add_pending("XAUUSD", 555)
    t = T0 + 1
    while t < T0 + 3600:
        br.advance(t); tr.step(t)
        if tr.own_positions:
            break
        t += 0.3
    assert tr.own_positions, "no bot position opened to test STOP"
    r = tr.cmd_stop()
    assert r["ok"], r
    assert not [p for p in br.positions_.values() if p["magic"] == cfg.magic]
    assert manual in br.positions_ and other_ea in br.positions_
    assert own_pending not in br.orders_ and foreign_pending in br.orders_
    sent = br.sent_orders
    run(tr, br, t + 0.3, t + 900)
    assert br.sent_orders == sent, "entries after STOP"
    assert not tr.running


def test_restart_recovery_readopts_positions(tmp_path):
    cfg = EngineConfig()
    ticks = gen_ticks(5, T0, 3600)
    tr, br = make(tmp_path, {"XAUUSD": ticks}, cfg)
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    t = T0 + 1
    while not tr.own_positions and t < T0 + 3600:
        br.advance(t); tr.step(t); t += 0.3
    tickets = {int(p["ticket"]) for p in tr.own_positions}
    assert tickets
    # simulate engine crash: new Trader process, same data dir, same terminal/account
    from rapidburst.secure_store import SecureStore
    from rapidburst.trader import Trader
    store2 = SecureStore(str(tmp_path))
    store2.save_credentials({"login": 12345678, "password": "test-only", "server": "Sim-Demo"})
    tr2 = Trader(br, store2, cfg, str(tmp_path))
    assert not tr2.running, "must never resume trading from persisted state"
    assert tr2.selected == ["XAUUSD"]
    sent = br.sent_orders
    r = tr2.cmd_start()      # explicit START (sent by the launcher)
    assert r["ok"], r
    assert tickets <= set(tr2.pos_state), "positions not re-adopted on START"
    assert any("Re-adopted" in l["msg"] for l in tr2.logs)
    # idempotent: a second START does not open a new batch
    assert tr2.cmd_start()["msg"] == "already running"
    # warm-up on fresh ticks only -> no entries in the first 60 s
    run(tr2, br, t, t + 55)
    assert br.sent_orders == sent, "entered on stale state before fresh warm-up"
    eng = tr2.engines["XAUUSD"]
    assert eng.buf.ticks and eng.buf.ticks[0].t > t - 0.5, "engine used ticks from before START"
    run(tr2, br, t + 55, t + 400)
    # adopted positions are managed (exited) by the new process
    assert not (tickets & {int(p["ticket"]) for p in br.positions_.values()})


def test_stop_when_disconnected_reconnects_then_flattens_and_shutdown(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 3600)}, cfg)
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    t = T0 + 1
    while not tr.own_positions and t < T0 + 3600:
        br.advance(t); tr.step(t); t += 0.3
    assert tr.own_positions
    assert not tr.cmd_shutdown()["ok"], "shutdown allowed while running"
    br.connected_flag = False          # terminal lost connection
    tr.conn_state = "DISCONNECTED"
    r = tr.cmd_stop()
    assert r["ok"], r
    assert not [p for p in br.positions_.values() if p["magic"] == cfg.magic]
    assert tr.cmd_shutdown()["ok"]
    assert tr.stop_event.is_set() and br.shutdown_calls >= 1


def test_disconnect_pauses_and_reconnects(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 1800)})
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    run(tr, br, T0 + 1, T0 + 200)
    br.drop = True
    sent = br.sent_orders
    run(tr, br, T0 + 200, T0 + 400)
    assert tr.conn_state in ("DISCONNECTED", "ERROR")
    assert br.sent_orders == sent
    br.drop = False
    run(tr, br, T0 + 400, T0 + 700)
    assert tr.conn_state == "CONNECTED" and tr.running
    assert br.login_calls >= 2


def test_daily_loss_limit_halts_and_flattens(tmp_path):
    cfg = EngineConfig(daily_loss_limit_pct=0.05)   # $5 on a $10k test account
    # pure noise (no momentum bursts) + slippage: the engine should lose here, and the limit must stop it
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 7200, burst_every=1e9)}, cfg, slippage_pts=5)
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    run(tr, br, T0 + 1, T0 + 7200)
    assert tr.risk.halted and "daily loss" in tr.risk.halt_reason
    assert not [p for p in br.positions_.values() if p["magic"] == cfg.magic]
    assert not tr.cmd_start()["ok"]


def test_netting_account_does_not_touch_manual_symbol(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(5, T0, 1800)}, cfg, hedging=False)
    br.advance(T0 + 1)
    manual = br.add_foreign_position("XAUUSD", "BUY", 0.1, magic=0)
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    run(tr, br, T0 + 1, T0 + 1800)
    assert br.sent_orders == 0 and manual in br.positions_


def test_encrypted_connect_roundtrip(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": gen_ticks(1, T0, 300)})
    tr.store.forget_credentials()
    pub = serialization.load_der_public_key(base64.b64decode(tr.store.public_key_spki_b64()))
    ct = pub.encrypt(json.dumps({"login": "5550001", "password": "p@ss", "server": "Sim-Demo"}).encode(),
                     padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
    br.advance(T0 + 1)
    r = tr.cmd_connect(base64.b64encode(ct).decode())
    assert r["ok"] and tr.conn_state == "CONNECTED" and br.login == 5550001
    assert all("p@ss" not in l["msg"] for l in tr.logs)
    bad = tr.cmd_connect(base64.b64encode(b"x" * 384).decode())
    assert not bad["ok"]


def test_rejected_order_is_not_retried_in_a_loop(tmp_path):
    tr, br = make(tmp_path, {"XAUUSD": burst_series(T0, 1)})
    assert connect_and_start(tr, br, T0 + 1)["ok"]
    br.reject_next = "Market closed"
    t = T0 + 1
    while br.sent_orders == 0 and t < T0 + 160:
        br.advance(t); tr.step(t); t += 0.15
    assert br.sent_orders == 1
    sent_at = t
    run(tr, br, t, t + 3, dt=0.15)
    assert br.sent_orders == 1, "retry within cooldown"
